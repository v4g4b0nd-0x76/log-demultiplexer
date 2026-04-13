use std::sync::Arc;

use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

use crate::{
    MultiplexerError,
    conf::{Config, ParseType, ParsedBatch},
};

const MIN_PARALLEL_BATCH_LEN: usize = 128;

pub struct Parser {
    dispatcher: JoinHandle<()>,
    workers: Vec<JoinHandle<()>>,
}

impl Parser {
    pub async fn wait(self) {
        let _ = self.dispatcher.await;
        for worker in self.workers {
            let _ = worker.await;
        }
    }
}

pub fn start_parser(
    conf: Arc<Config>,
    rx: UnboundedReceiver<Arc<[u8]>>,
    d_tx: UnboundedSender<ParsedBatch>,
) -> Result<Parser, MultiplexerError> {
   

    let worker_count = conf.conn_workers;
    let batch_threshold = conf.parse_batch_thresh.max(1);
    let use_rayon = worker_count == 1 && rayon::current_num_threads() > 1;
    let mut worker_txs = Vec::with_capacity(worker_count);
    let mut workers = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let (tx, mut worker_rx) = tokio::sync::mpsc::unbounded_channel::<Arc<[u8]>>();
        worker_txs.push(tx);

        let d_tx = d_tx.clone();
        let parse_type = conf.parse_type.clone();
        workers.push(tokio::spawn(async move {
            let mut batch = Vec::with_capacity(batch_threshold);
            while let Some(data) = worker_rx.recv().await {
                batch.push(data);
                if batch.len() == batch_threshold {
                    flush_batch(&mut batch, &parse_type, &d_tx, use_rayon);
                }
            }

            if !batch.is_empty() {
                flush_batch(&mut batch, &parse_type, &d_tx, use_rayon);
            }
        }));
    }

    let dispatcher = tokio::spawn(async move {
        let mut rx = rx;
        let mut worker_idx = 0usize;
        while let Some(data) = rx.recv().await {
            if worker_txs[worker_idx].send(data).is_err() {
                break;
            }
            worker_idx = (worker_idx + 1) % worker_txs.len();
        }
    });

    Ok(Parser {
        dispatcher,
        workers,
    })
}

fn flush_batch(
    batch: &mut Vec<Arc<[u8]>>,
    parse_type: &ParseType,
    d_tx: &UnboundedSender<ParsedBatch>,
    use_rayon: bool,
) {
    let next_capacity = batch.capacity().max(1);
    let payloads = std::mem::replace(batch, Vec::with_capacity(next_capacity));
    let parsed = parse_batch(payloads, parse_type, use_rayon);
    let _ = d_tx.send(parsed);
}

fn parse_batch(batch: Vec<Arc<[u8]>>, parse_type: &ParseType, use_rayon: bool) -> ParsedBatch {
    match parse_type {
        ParseType::Str => ParsedBatch::Str(parse_strings(batch, use_rayon)),
        ParseType::Json => ParsedBatch::Json(parse_json(batch, use_rayon)),
    }
}

fn parse_strings(batch: Vec<Arc<[u8]>>, use_rayon: bool) -> Vec<String> {
    if use_rayon && batch.len() >= MIN_PARALLEL_BATCH_LEN {
        batch
            .into_par_iter()
            .map(|data| String::from_utf8_lossy(&data).into_owned())
            .collect()
    } else {
        batch
            .into_iter()
            .map(|data| String::from_utf8_lossy(&data).into_owned())
            .collect()
    }
}

fn parse_json(batch: Vec<Arc<[u8]>>, use_rayon: bool) -> Vec<serde_json::Value> {
    if use_rayon && batch.len() >= MIN_PARALLEL_BATCH_LEN {
        batch
            .into_par_iter()
            .map(|data| serde_json::from_slice(&data).unwrap())
            .collect()
    } else {
        batch
            .into_iter()
            .map(|data| serde_json::from_slice(&data).unwrap())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::mpsc;

    use super::start_parser;
    use crate::conf::{Config, ParseType, ParsedBatch};

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    fn make_conf(workers: usize, threshold: usize, parse_type: ParseType) -> Arc<Config> {
        Arc::new(Config {
            conn_workers: workers,
            udp_buffer_size: 1024,
            port: 5402,
            parse_batch_thresh: threshold,
            parse_type,
        })
    }

    fn payload(bytes: &[u8]) -> Arc<[u8]> {
        Arc::from(bytes)
    }

    async fn recv_batch(rx: &mut tokio::sync::mpsc::UnboundedReceiver<ParsedBatch>) -> ParsedBatch {
        tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("parser timed out")
            .expect("parser channel closed")
    }

    #[tokio::test]
    async fn flushes_once_threshold_is_reached() {
        let conf = make_conf(1, 2, ParseType::Str);
        let (tx, rx) = mpsc::unbounded_channel::<Arc<[u8]>>();
        let (d_tx, mut d_rx) = mpsc::unbounded_channel::<ParsedBatch>();

        let parser = start_parser(conf, rx, d_tx).unwrap();
        tx.send(payload(b"alpha")).unwrap();
        tx.send(payload(b"beta")).unwrap();

        let batch = recv_batch(&mut d_rx).await;
        assert_eq!(
            batch,
            ParsedBatch::Str(vec!["alpha".to_string(), "beta".to_string()])
        );

        drop(tx);
        parser.wait().await;
    }

    #[tokio::test]
    async fn flushes_remaining_items_when_input_channel_closes() {
        let conf = make_conf(1, 4, ParseType::Str);
        let (tx, rx) = mpsc::unbounded_channel::<Arc<[u8]>>();
        let (d_tx, mut d_rx) = mpsc::unbounded_channel::<ParsedBatch>();

        let parser = start_parser(conf, rx, d_tx).unwrap();
        tx.send(payload(b"left")).unwrap();
        tx.send(payload(b"over")).unwrap();
        drop(tx);

        let batch = recv_batch(&mut d_rx).await;
        assert_eq!(
            batch,
            ParsedBatch::Str(vec!["left".to_string(), "over".to_string()])
        );

        parser.wait().await;
    }

    #[tokio::test]
    async fn parses_json_batches() {
        let conf = make_conf(1, 2, ParseType::Json);
        let (tx, rx) = mpsc::unbounded_channel::<Arc<[u8]>>();
        let (d_tx, mut d_rx) = mpsc::unbounded_channel::<ParsedBatch>();

        let parser = start_parser(conf, rx, d_tx).unwrap();
        tx.send(payload(br#"{"message":"one","value":1}"#)).unwrap();
        tx.send(payload(br#"{"message":"two","value":2}"#)).unwrap();

        let batch = recv_batch(&mut d_rx).await;
        assert_eq!(
            batch,
            ParsedBatch::Json(vec![
                serde_json::json!({"message": "one", "value": 1}),
                serde_json::json!({"message": "two", "value": 2}),
            ])
        );

        drop(tx);
        parser.wait().await;
    }

    #[tokio::test]
    async fn distributes_work_round_robin_across_workers() {
        let conf = make_conf(2, 2, ParseType::Str);
        let (tx, rx) = mpsc::unbounded_channel::<Arc<[u8]>>();
        let (d_tx, mut d_rx) = mpsc::unbounded_channel::<ParsedBatch>();

        let parser = start_parser(conf, rx, d_tx).unwrap();
        for item in [b"a1", b"b1", b"a2", b"b2"] {
            tx.send(payload(item)).unwrap();
        }
        drop(tx);

        let mut batches = vec![recv_batch(&mut d_rx).await, recv_batch(&mut d_rx).await];
        batches.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));

        assert_eq!(
            batches,
            vec![
                ParsedBatch::Str(vec!["a1".to_string(), "a2".to_string()]),
                ParsedBatch::Str(vec!["b1".to_string(), "b2".to_string()]),
            ]
        );

        parser.wait().await;
    }

    #[test]
    fn rejects_zero_workers() {
        let conf = make_conf(0, 1, ParseType::Str);
        let (_tx, rx) = mpsc::unbounded_channel::<Arc<[u8]>>();
        let (d_tx, _d_rx) = mpsc::unbounded_channel::<ParsedBatch>();

        match start_parser(conf, rx, d_tx) {
            Err(crate::MultiplexerError::SpawnWorker((0, _))) => {}
            Err(other) => panic!("unexpected error variant: {other:?}"),
            Ok(_) => panic!("zero workers must be rejected"),
        }
    }
}
