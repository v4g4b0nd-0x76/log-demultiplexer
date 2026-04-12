use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use log_demultiplexer::conf::Config;
use log_demultiplexer::udp_listener::start_udp_listener;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const PAYLOAD_SIZES: &[usize] = &[64, 512, 1024, 8192];

/// Start listener on `port`, send one datagram of `payload`, drain it.
async fn send_and_drain_one(port: usize, payload: &[u8]) {
    let conf = Arc::new(Config {
        conn_workers: 1,
        udp_buffer_size: 65_535,
        port: 5403,
    });
    let cancel = Arc::new(CancellationToken::new());
    let (tx, mut rx) = mpsc::unbounded_channel::<Arc<[u8]>>();

    start_udp_listener(conf, cancel.clone(), tx, port).unwrap();

    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(payload, format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
        .await
        .expect("bench recv timeout")
        .unwrap();

    cancel.cancel();
}

/// Start listener on `port` with `workers`, send `count` datagrams, drain all.
async fn send_and_drain_many(port: usize, payload: &[u8], workers: usize, count: usize) {
    let conf = Arc::new(Config {
        conn_workers: workers,
        udp_buffer_size: 65_535,
        port: 5403,
    });
    let cancel = Arc::new(CancellationToken::new());
    let (tx, mut rx) = mpsc::unbounded_channel::<Arc<[u8]>>();

    start_udp_listener(conf, cancel.clone(), tx, port).unwrap();

    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dst = format!("127.0.0.1:{port}");
    for _ in 0..count {
        sender.send_to(payload, &dst).await.unwrap();
    }
    for _ in 0..count {
        tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("bench recv timeout")
            .unwrap();
    }

    cancel.cancel();
}

/// Single-worker throughput for payloads of 64 / 512 / 1024 / 8192 bytes.
fn bench_single_worker_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("udp_single_worker");
    for &size in PAYLOAD_SIZES {
        let payload = vec![0xABu8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            b.to_async(&rt).iter(|| send_and_drain_one(15401, payload));
        });
    }
    group.finish();
}

/// Fan-out across 1 / 2 / 4 workers — 100 × 1 KB packets per iteration.
fn bench_multi_worker_fanout(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("udp_multi_worker");
    const COUNT: usize = 100;
    let payload = vec![0xCDu8; 1024];
    group.throughput(Throughput::Elements(COUNT as u64));

    for workers in [1usize, 2, 4] {
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.to_async(&rt)
                    .iter(|| send_and_drain_many(15501, &payload, workers, COUNT));
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_single_worker_throughput,
    bench_multi_worker_fanout
);
criterion_main!(benches);
