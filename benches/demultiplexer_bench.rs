use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use log_demultiplexer::{
    conf::{Config, ParseType, ParsedBatch},
    consumers::{Consumer, ConsumerType, Consumers},
    demultiplexer::{Demultiplexer, DemultiplexerMetrics, start_demultiplexer},
};
use tokio::sync::{
    RwLock,
    mpsc::{self, UnboundedSender},
};
use tokio_util::sync::CancellationToken;

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const PAYLOAD_BATCHES: usize = 64;

struct DemultiplexerBenchHarness {
    input_tx: UnboundedSender<ParsedBatch>,
    demultiplexer: Option<Demultiplexer>,
    metrics: Arc<DemultiplexerMetrics>,
    cancel_token: Arc<CancellationToken>,
    consumer_count: usize,
}

impl DemultiplexerBenchHarness {
    async fn new(consumer_count: usize) -> Self {
        let conf = Arc::new(Config {
            udp_buffer_size: 65_535,
            conn_workers: 1,
            port: 5401,
            parse_batch_thresh: 64,
            parse_type: ParseType::Str,
        });
        let consumers = Arc::new(RwLock::new(Consumers {
            consumers: (0..consumer_count)
                .map(|idx| Consumer {
                    consumer_type: ConsumerType::Redis,
                    addr: format!("127.0.0.1:{}", 6300 + idx),
                    username: None,
                    password: None,
                    target_name: Some("bench".to_string()),
                    enable: true,
                })
                .collect(),
        }));
        let cancel_token = Arc::new(CancellationToken::new());
        let (input_tx, input_rx) = mpsc::unbounded_channel::<ParsedBatch>();
        let demultiplexer =
            start_demultiplexer(conf, Arc::clone(&cancel_token), input_rx, consumers)
                .await
                .unwrap();
        let metrics = demultiplexer.metrics();

        tokio::time::timeout(WAIT_TIMEOUT, async {
            while metrics.active_workers() != consumer_count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("demultiplexer workers did not start");

        Self {
            input_tx,
            demultiplexer: Some(demultiplexer),
            metrics,
            cancel_token,
            consumer_count,
        }
    }

    async fn run_round(&mut self, payloads: &[ParsedBatch]) {
        let target = self.metrics.deliveries() + (payloads.len() * self.consumer_count) as u64;
        for payload in payloads {
            self.input_tx.send(payload.clone()).unwrap();
        }

        tokio::time::timeout(WAIT_TIMEOUT, async {
            while self.metrics.deliveries() < target {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("demultiplexer did not drain payloads");
    }

    async fn shutdown(mut self) {
        self.cancel_token.cancel();
        drop(self.input_tx);
        if let Some(demultiplexer) = self.demultiplexer.take() {
            demultiplexer.wait().await;
        }
    }
}

fn make_string_batch(entry_count: usize, entry_size: usize) -> ParsedBatch {
    ParsedBatch::Str(vec!["x".repeat(entry_size); entry_count])
}

fn bench_consumer_fanout(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("demultiplexer_fanout");
    let payloads = vec![make_string_batch(32, 128); PAYLOAD_BATCHES];

    for consumer_count in [1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(
            (PAYLOAD_BATCHES * consumer_count) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(consumer_count),
            &consumer_count,
            |b, &consumer_count| {
                let mut harness = rt.block_on(DemultiplexerBenchHarness::new(consumer_count));
                b.iter_custom(|iters| {
                    let started = Instant::now();
                    for _ in 0..iters {
                        rt.block_on(harness.run_round(&payloads));
                    }
                    started.elapsed()
                });
                rt.block_on(harness.shutdown());
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_consumer_fanout);
criterion_main!(benches);
