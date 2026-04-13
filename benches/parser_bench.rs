use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use log_demultiplexer::{
    conf::{Config, ParseType, ParsedBatch},
    parser::{Parser, start_parser},
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

const BATCH_LEN: usize = 256;
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

struct ParserBenchHarness {
    input_tx: UnboundedSender<Arc<[u8]>>,
    output_rx: UnboundedReceiver<ParsedBatch>,
    parser: Option<Parser>,
    expected_batches: usize,
}

impl ParserBenchHarness {
    async fn new(parse_type: ParseType, workers: usize, batch_len: usize) -> Self {
        let conf = Arc::new(Config {
            conn_workers: workers,
            udp_buffer_size: 65_535,
            port: 5403,
            parse_batch_thresh: batch_len,
            parse_type,
        });
        let (input_tx, input_rx) = mpsc::unbounded_channel::<Arc<[u8]>>();
        let (output_tx, output_rx) = mpsc::unbounded_channel::<ParsedBatch>();
        let cancel_token = CancellationToken::new();
        let parser = start_parser(conf, Arc::new(cancel_token), input_rx, output_tx).unwrap();

        Self {
            input_tx,
            output_rx,
            parser: Some(parser),
            expected_batches: workers,
        }
    }

    async fn run_round(&mut self, payloads: &[Arc<[u8]>]) {
        for payload in payloads {
            self.input_tx.send(payload.clone()).unwrap();
        }

        for _ in 0..self.expected_batches {
            let batch = tokio::time::timeout(RECV_TIMEOUT, self.output_rx.recv())
                .await
                .expect("bench recv timeout")
                .expect("parser output channel closed");
            criterion::black_box(batch);
        }
    }

    async fn shutdown(mut self) {
        drop(self.input_tx);
        if let Some(parser) = self.parser.take() {
            parser.wait().await;
        }
    }
}

fn make_string_payload(size: usize) -> Arc<[u8]> {
    Arc::from(vec![b'x'; size].into_boxed_slice())
}

fn make_json_payload(size: usize) -> Arc<[u8]> {
    let overhead = br#"{"level":"INFO","message":"","service":"bench","count":1}"#.len();
    let message_len = size.saturating_sub(overhead).max(1);
    let payload = format!(
        r#"{{"level":"INFO","message":"{}","service":"bench","count":1}}"#,
        "x".repeat(message_len)
    );
    Arc::from(payload.into_bytes().into_boxed_slice())
}

fn bench_string_parser(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("parser_str");

    for &size in &[64usize, 512, 2048] {
        let payloads = vec![make_string_payload(size); BATCH_LEN];
        group.throughput(Throughput::Bytes((size * BATCH_LEN) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &payloads,
            |b, payloads| {
                let mut harness =
                    rt.block_on(ParserBenchHarness::new(ParseType::Str, 1, BATCH_LEN));
                b.iter_custom(|iters| {
                    let started = Instant::now();
                    for _ in 0..iters {
                        rt.block_on(harness.run_round(payloads));
                    }
                    started.elapsed()
                });
                rt.block_on(harness.shutdown());
            },
        );
    }

    group.finish();
}

fn bench_json_parser(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("parser_json");

    for &size in &[128usize, 1024, 4096] {
        let payloads = vec![make_json_payload(size); BATCH_LEN];
        let bytes = payloads
            .first()
            .map(|payload| payload.len())
            .unwrap_or_default();
        group.throughput(Throughput::Bytes((bytes * BATCH_LEN) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &payloads,
            |b, payloads| {
                let mut harness =
                    rt.block_on(ParserBenchHarness::new(ParseType::Json, 1, BATCH_LEN));
                b.iter_custom(|iters| {
                    let started = Instant::now();
                    for _ in 0..iters {
                        rt.block_on(harness.run_round(payloads));
                    }
                    started.elapsed()
                });
                rt.block_on(harness.shutdown());
            },
        );
    }

    group.finish();
}

fn bench_parser_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("parser_scaling");

    for &(label, ref parse_type, payload_size) in &[
        ("str_1024b", ParseType::Str, 1024usize),
        ("json_1024b", ParseType::Json, 1024usize),
    ] {
        for workers in [1usize, 2, 4] {
            let payload = match parse_type {
                ParseType::Str => make_string_payload(payload_size),
                ParseType::Json => make_json_payload(payload_size),
            };
            let payloads = vec![payload; BATCH_LEN * workers];
            let total_bytes =
                payloads.first().map(|item| item.len()).unwrap_or_default() * payloads.len();

            group.throughput(Throughput::Bytes(total_bytes as u64));
            group.bench_with_input(BenchmarkId::new(label, workers), &workers, |b, &workers| {
                let mut harness = rt.block_on(ParserBenchHarness::new(
                    parse_type.clone(),
                    workers,
                    BATCH_LEN,
                ));
                b.iter_custom(|iters| {
                    let started = Instant::now();
                    for _ in 0..iters {
                        rt.block_on(harness.run_round(&payloads));
                    }
                    started.elapsed()
                });
                rt.block_on(harness.shutdown());
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_string_parser,
    bench_json_parser,
    bench_parser_scaling
);
criterion_main!(benches);
