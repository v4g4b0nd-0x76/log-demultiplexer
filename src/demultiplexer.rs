use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bb8::Pool;
use bb8_redis::{RedisConnectionManager, redis};
use elasticsearch::{
    Elasticsearch,
    auth::Credentials,
    cert::CertificateValidation,
    http::transport::{self, SingleNodeConnectionPool, Transport, TransportBuilder},
};
use tokio::{
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use urlencoding::encode;

use crate::{
    MultiplexerError,
    conf::{Config, ParsedBatch},
    consumers::{Consumer, Consumers},
};

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const HEALTHCHECK_INTERVAL: Duration = Duration::from_secs(5);
const HEALTHCHECK_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Default)]
pub struct DemultiplexerMetrics {
    input_batches: AtomicU64,
    deliveries: AtomicU64,
    active_workers: AtomicUsize,
}

impl DemultiplexerMetrics {
    pub fn input_batches(&self) -> u64 {
        self.input_batches.load(Ordering::Relaxed)
    }

    pub fn deliveries(&self) -> u64 {
        self.deliveries.load(Ordering::Relaxed)
    }

    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Relaxed)
    }
}

pub struct Demultiplexer {
    manager: JoinHandle<()>,
    metrics: Arc<DemultiplexerMetrics>,
}

impl Demultiplexer {
    pub async fn wait(self) {
        let _ = self.manager.await;
    }

    pub fn metrics(&self) -> Arc<DemultiplexerMetrics> {
        Arc::clone(&self.metrics)
    }
}

#[derive(Clone, Copy)]
struct RuntimeOptions {
    reconcile_interval: Duration,
    healthcheck_interval: Duration,
    healthcheck_backoff: Duration,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            reconcile_interval: RECONCILE_INTERVAL,
            healthcheck_interval: HEALTHCHECK_INTERVAL,
            healthcheck_backoff: HEALTHCHECK_BACKOFF,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone)]
struct DeliveryEvent {
    consumer_key: String,
    batch: Arc<ParsedBatch>,
}

struct ConsumerRuntime {
    tx: UnboundedSender<Arc<ParsedBatch>>,
    stop_token: CancellationToken,
    worker: JoinHandle<()>,
    healthcheck: JoinHandle<()>,
}

pub async fn start_demultiplexer(
    _conf: Arc<Config>,
    cancel_token: Arc<CancellationToken>,
    rx: UnboundedReceiver<ParsedBatch>,
    consumers: Arc<RwLock<Consumers>>,
) -> Result<Demultiplexer, MultiplexerError> {
    start_demultiplexer_with_options(cancel_token, rx, consumers, RuntimeOptions::default(), None)
        .await
}

async fn start_demultiplexer_with_options(
    cancel_token: Arc<CancellationToken>,
    rx: UnboundedReceiver<ParsedBatch>,
    consumers: Arc<RwLock<Consumers>>,
    options: RuntimeOptions,
    observer: Option<UnboundedSender<DeliveryEvent>>,
) -> Result<Demultiplexer, MultiplexerError> {
    let metrics = Arc::new(DemultiplexerMetrics::default());
    let manager_metrics = Arc::clone(&metrics);
    let manager = tokio::spawn(async move {
        let mut runtimes = HashMap::<String, ConsumerRuntime>::new();
        let mut rx = rx;
        let mut reconcile_tick = tokio::time::interval(options.reconcile_interval);
        reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let initial = consumers.read().await.clone();
        reconcile_consumers(
            &initial,
            &cancel_token,
            &mut runtimes,
            &manager_metrics,
            options,
            observer.clone(),
        )
        .await;

        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                _ = reconcile_tick.tick() => {
                    let snapshot = consumers.read().await.clone();
                    reconcile_consumers(
                        &snapshot,
                        &cancel_token,
                        &mut runtimes,
                        &manager_metrics,
                        options,
                        observer.clone(),
                    ).await;
                }
                batch = rx.recv() => {
                    let Some(batch) = batch else {
                        break;
                    };
                    manager_metrics.input_batches.fetch_add(1, Ordering::Relaxed);
                    let shared = Arc::new(batch);
                    let dead_keys = dispatch_batch(&runtimes, shared);
                    for dead_key in dead_keys {
                        if let Some(runtime) = runtimes.remove(&dead_key) {
                            stop_runtime(runtime, &manager_metrics).await;
                        }
                    }
                }
            }
        }

        for runtime in runtimes.drain().map(|(_, runtime)| runtime) {
            stop_runtime(runtime, &manager_metrics).await;
        }
    });

    Ok(Demultiplexer { manager, metrics })
}

fn dispatch_batch(
    runtimes: &HashMap<String, ConsumerRuntime>,
    batch: Arc<ParsedBatch>,
) -> Vec<String> {
    let mut dead_keys = Vec::new();
    for (consumer_key, runtime) in runtimes {
        if runtime.tx.send(Arc::clone(&batch)).is_err() {
            dead_keys.push(consumer_key.clone());
        }
    }
    dead_keys
}

async fn reconcile_consumers(
    consumers: &Consumers,
    cancel_token: &CancellationToken,
    runtimes: &mut HashMap<String, ConsumerRuntime>,
    metrics: &Arc<DemultiplexerMetrics>,
    options: RuntimeOptions,
    observer: Option<UnboundedSender<DeliveryEvent>>,
) {
    let mut desired = HashSet::new();

    for consumer in consumers
        .consumers
        .iter()
        .filter(|consumer| consumer.enable)
    {
        let key = consumer_key(consumer);
        desired.insert(key.clone());

        if !runtimes.contains_key(&key) {
            let runtime = spawn_consumer_runtime(
                key.clone(),
                consumer.clone(),
                cancel_token,
                Arc::clone(metrics),
                options,
                observer.clone(),
            );
            metrics.active_workers.fetch_add(1, Ordering::Relaxed);
            runtimes.insert(key, runtime);
        }
    }

    let stale: Vec<_> = runtimes
        .keys()
        .filter(|key| !desired.contains(*key))
        .cloned()
        .collect();

    for consumer_key in stale {
        if let Some(runtime) = runtimes.remove(&consumer_key) {
            stop_runtime(runtime, metrics).await;
        }
    }
}

fn spawn_consumer_runtime(
    consumer_key: String,
    consumer: Consumer,
    cancel_token: &CancellationToken,
    metrics: Arc<DemultiplexerMetrics>,
    options: RuntimeOptions,
    observer: Option<UnboundedSender<DeliveryEvent>>,
) -> ConsumerRuntime {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<ParsedBatch>>();
    let stop_token = cancel_token.child_token();
    let consumer = Arc::new(consumer);
    let consumer_type = Arc::new(consumer.consumer_type.clone());

    let worker_token = stop_token.clone();
    let worker_key = consumer_key.clone();
    let worker_metrics = Arc::clone(&metrics);
    let worker_observer = observer.clone();
    let worker_consumer = Arc::clone(&consumer);
    let worker_consumer_type = Arc::clone(&consumer_type);
    let worker = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = worker_token.cancelled() => break,
                batch = rx.recv() => {
                    let Some(batch) = batch else {
                        break;
                    };
                    worker_metrics.deliveries.fetch_add(1, Ordering::Relaxed);
                    if let Some(observer) = worker_observer.as_ref() {
                        let _ = observer.send(DeliveryEvent {
                            consumer_key: worker_key.clone(),
                            batch: Arc::clone(&batch),
                        });
                    }

                    match worker_consumer_type.as_ref() {
                        crate::consumers::ConsumerType::Elastic => index_elastic_batch(&worker_consumer, batch).await.is_ok(),
                        crate::consumers::ConsumerType::Redis => push_redis_batch(&worker_consumer, batch).await.is_ok(),
                    };
                }
            }
        }
    });

    let healthcheck_token = stop_token.clone();
    let healthcheck_key = consumer_key.clone();
    let healthcheck_consumer = Arc::clone(&consumer);
    let healthcheck_consumer_type = Arc::clone(&consumer_type);
    let healthcheck = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(options.healthcheck_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut consecutive_failures = 0u8;

        loop {
            tokio::select! {
                biased;
                _ = healthcheck_token.cancelled() => break,
                _ = ticker.tick() => {
                    let healthy = match healthcheck_consumer_type.as_ref() {
                        crate::consumers::ConsumerType::Elastic => elastic_health_check(&healthcheck_consumer).await.is_ok(),
                        crate::consumers::ConsumerType::Redis => redis_health_check(&healthcheck_consumer).await.is_ok(),
                    };
                    if healthy {
                        consecutive_failures = 0;
                        continue;
                    }

                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures > 3 {
                        eprintln!(
                            "consumer worker {healthcheck_key} ({healthcheck_consumer_type:?}) failed healthchecks; pausing for {:?}",
                            options.healthcheck_backoff
                        );
                        tokio::time::sleep(options.healthcheck_backoff).await;
                        consecutive_failures = 0;
                    }
                }
            }
        }
    });

    ConsumerRuntime {
        tx,
        stop_token,
        worker,
        healthcheck,
    }
}

async fn stop_runtime(runtime: ConsumerRuntime, metrics: &DemultiplexerMetrics) {
    runtime.stop_token.cancel();
    drop(runtime.tx);
    let _ = runtime.worker.await;
    let _ = runtime.healthcheck.await;
    metrics.active_workers.fetch_sub(1, Ordering::Relaxed);
}

fn consumer_key(consumer: &Consumer) -> String {
    format!(
        "{:?}|{}|{}|{}|{}",
        consumer.consumer_type,
        consumer.addr,
        consumer.username.as_deref().unwrap_or_default(),
        consumer.password.as_deref().unwrap_or_default(),
        consumer.target_name.as_deref().unwrap_or_default(),
    )
}

async fn elastic_health_check(consumer: &Consumer) -> Result<bool, MultiplexerError> {
    let client = create_elastic_client(consumer)?;
    let res = client
        .ping()
        .send()
        .await
        .map_err(|e| MultiplexerError::ElasticClient(e.to_string()))?;

    match res.status_code().as_u16() {
        200 | 403 => Ok(true),
        code => Err(MultiplexerError::ElasticClient(format!(
            "ping unexpected status code: {}",
            code
        ))),
    }
}

async fn index_elastic_batch(
    consumer: &Consumer,
    batch: Arc<ParsedBatch>,
) -> Result<(), MultiplexerError> {
    let client = create_elastic_client(consumer)?;

    let index = match &consumer.target_name {
        Some(name) => format!("{}-{}", name, chrono::Utc::now().format("%Y.%m.%d")),
        None => return Err(MultiplexerError::ElasticClient("no target_name".into())),
    };

    let body: Vec<elasticsearch::BulkOperation<serde_json::Value>> = match batch.as_ref() {
        ParsedBatch::Str(items) => items
            .iter()
            .map(|item| {
                let doc = serde_json::from_str(item)
                    .unwrap_or_else(|_| serde_json::json!({ "raw": item }));
                elasticsearch::BulkOperation::index(doc).into()
            })
            .collect(),
        ParsedBatch::Json(items) => items
            .iter()
            .map(|doc| elasticsearch::BulkOperation::index(doc.clone()).into())
            .collect(),
    };

    client
        .bulk(elasticsearch::BulkParts::Index(&index))
        .body(body)
        .send()
        .await
        .map_err(|e| MultiplexerError::ElasticClient(e.to_string()))?;

    Ok(())
}


async fn push_redis_batch(
    consumer: &Consumer,
    batch: Arc<ParsedBatch>,
) -> Result<(), MultiplexerError> {
    let url = match &consumer.password {
        Some(pass) => format!("redis://:{}@{}", encode(pass), consumer.addr),
        None => format!("redis://{}", consumer.addr),
    };

    let list = match &consumer.target_name {
        Some(name) => format!("{}-{}", name, chrono::Utc::now().format("%Y.%m.%d")),
        None => return Err(MultiplexerError::RedisClient("no target_name".into())),
    };

    let manager = RedisConnectionManager::new(url)
        .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;

    let pool = Pool::builder()
        .max_size(8)
        .connection_timeout(Duration::from_secs(2))
        .build(manager)
        .await
        .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;

    let values: Vec<Vec<u8>> = match batch.as_ref() {
        ParsedBatch::Str(items) => items.iter().map(|s| s.as_bytes().to_vec()).collect(),
        ParsedBatch::Json(items) => items.iter().map(|v| v.to_string().into_bytes()).collect(),
    };

    let chunks: Vec<&[Vec<u8>]> = values.chunks(10_000).collect();

    let tasks: Vec<_> = chunks
        .into_iter()
        .map(|chunk| {
            let pool = pool.clone();
            let list = list.clone();
            let chunk = chunk.to_vec();
            tokio::spawn(async move {
                let mut conn = pool.get().await.map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;
                redis::cmd("RPUSH")
                    .arg(&list)
                    .arg(chunk)
                    .query_async::<()>(&mut *conn)
                    .await
                    .map_err(|e| MultiplexerError::RedisClient(e.to_string()))
            })
        })
        .collect();

    for task in tasks {
        task.await.map_err(|e| MultiplexerError::RedisClient(e.to_string()))??;
    }

    Ok(())
}

fn create_elastic_client(consumer: &Consumer) -> Result<Elasticsearch, MultiplexerError> {
    let url =
        Url::parse(&consumer.addr).map_err(|e| MultiplexerError::ElasticClient(e.to_string()))?;

    let transport = match (&consumer.username, &consumer.password) {
        (Some(user), Some(pass)) => TransportBuilder::new(SingleNodeConnectionPool::new(url))
            .auth(Credentials::Basic(user.clone(), pass.clone()))
            .cert_validation(CertificateValidation::None)
            .build(),
        _ => TransportBuilder::new(SingleNodeConnectionPool::new(url))
            .cert_validation(CertificateValidation::None)
            .build(),
    }
    .map_err(|e| MultiplexerError::ElasticClient(e.to_string()))?;

    Ok(Elasticsearch::new(transport))
}
async fn redis_health_check(consumer: &Consumer) -> Result<bool, MultiplexerError> {
    let url = match &consumer.password {
        Some(pass) => format!("redis://:{}@{}", encode(pass), consumer.addr),
        None => format!("redis://{}", consumer.addr),
    };

    let client =
        redis::Client::open(url).map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;

    let mut conn: redis::aio::MultiplexedConnection = tokio::time::timeout(
        Duration::from_secs(2),
        client.get_multiplexed_async_connection(),
    )
    .await
    .map_err(|_| MultiplexerError::RedisClient("connection timed out".into()))?
    .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;

    let pong: String = tokio::time::timeout(
        Duration::from_secs(2),
        redis::cmd("PING").query_async(&mut conn),
    )
    .await
    .map_err(|_| MultiplexerError::RedisClient("ping timed out".into()))?
    .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;

    Ok(pong == "PONG")
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::{
        RwLock,
        mpsc::{self, UnboundedReceiver},
    };
    use tokio_util::sync::CancellationToken;

    use super::{DeliveryEvent, RuntimeOptions, consumer_key, start_demultiplexer_with_options};
    use crate::{
        conf::ParsedBatch,
        consumers::{Consumer, ConsumerType, Consumers},
    };

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);
    const TEST_RECONCILE: Duration = Duration::from_millis(50);

    fn make_consumer(addr: &str, enable: bool) -> Consumer {
        Consumer {
            consumer_type: ConsumerType::Redis,
            addr: addr.to_string(),
            username: None,
            password: None,
            target_name: Some("logs".to_string()),
            enable,
        }
    }

    fn options() -> RuntimeOptions {
        RuntimeOptions {
            reconcile_interval: TEST_RECONCILE,
            healthcheck_interval: TEST_RECONCILE,
            healthcheck_backoff: Duration::from_millis(100),
        }
    }

    async fn recv_event(rx: &mut UnboundedReceiver<DeliveryEvent>) -> DeliveryEvent {
        tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("timed out waiting for demultiplexer event")
            .expect("demultiplexer observer channel closed")
    }

    #[tokio::test]
    async fn fans_out_shared_batches_to_all_enabled_consumers() {
        let consumers = Arc::new(RwLock::new(Consumers {
            consumers: vec![
                make_consumer("127.0.0.1:6379", true),
                make_consumer("127.0.0.1:6380", true),
            ],
        }));
        let cancel_token = Arc::new(CancellationToken::new());
        let (tx, rx) = mpsc::unbounded_channel::<ParsedBatch>();
        let (observer_tx, mut observer_rx) = mpsc::unbounded_channel::<DeliveryEvent>();

        let demux = start_demultiplexer_with_options(
            cancel_token.clone(),
            rx,
            consumers.clone(),
            options(),
            Some(observer_tx),
        )
        .await
        .unwrap();

        tx.send(ParsedBatch::Str(vec!["alpha".to_string()]))
            .unwrap();

        let event_a = recv_event(&mut observer_rx).await;
        let event_b = recv_event(&mut observer_rx).await;
        assert_ne!(event_a.consumer_key, event_b.consumer_key);
        assert!(Arc::ptr_eq(&event_a.batch, &event_b.batch));

        cancel_token.cancel();
        drop(tx);
        demux.wait().await;
    }

    #[tokio::test]
    async fn reconciles_enable_state_changes() {
        let first = make_consumer("127.0.0.1:6379", true);
        let second = make_consumer("127.0.0.1:6380", true);
        let first_key = consumer_key(&first);
        let second_key = consumer_key(&second);
        let consumers = Arc::new(RwLock::new(Consumers {
            consumers: vec![first.clone(), second.clone()],
        }));
        let cancel_token = Arc::new(CancellationToken::new());
        let (tx, rx) = mpsc::unbounded_channel::<ParsedBatch>();
        let (observer_tx, mut observer_rx) = mpsc::unbounded_channel::<DeliveryEvent>();

        let demux = start_demultiplexer_with_options(
            cancel_token.clone(),
            rx,
            consumers.clone(),
            options(),
            Some(observer_tx),
        )
        .await
        .unwrap();

        tx.send(ParsedBatch::Str(vec!["phase1".to_string()]))
            .unwrap();
        let mut phase_one = vec![
            recv_event(&mut observer_rx).await,
            recv_event(&mut observer_rx).await,
        ];
        phase_one.sort_by(|left, right| left.consumer_key.cmp(&right.consumer_key));
        assert_eq!(phase_one[0].consumer_key, first_key);
        assert_eq!(phase_one[1].consumer_key, second_key);

        consumers.write().await.consumers[1].enable = false;
        tokio::time::sleep(TEST_RECONCILE * 2).await;

        tx.send(ParsedBatch::Str(vec!["phase2".to_string()]))
            .unwrap();
        let phase_two = recv_event(&mut observer_rx).await;
        assert_eq!(phase_two.consumer_key, first_key);
        assert!(
            tokio::time::timeout(Duration::from_millis(150), observer_rx.recv())
                .await
                .is_err(),
            "disabled consumer should not receive batches"
        );

        consumers.write().await.consumers[1].enable = true;
        tokio::time::sleep(TEST_RECONCILE * 2).await;

        tx.send(ParsedBatch::Str(vec!["phase3".to_string()]))
            .unwrap();
        let mut phase_three = vec![
            recv_event(&mut observer_rx).await,
            recv_event(&mut observer_rx).await,
        ];
        phase_three.sort_by(|left, right| left.consumer_key.cmp(&right.consumer_key));
        assert_eq!(phase_three[0].consumer_key, first_key);
        assert_eq!(phase_three[1].consumer_key, second_key);

        cancel_token.cancel();
        drop(tx);
        demux.wait().await;
    }

    #[tokio::test]
    async fn cancellation_stops_dispatch_and_workers() {
        let consumers = Arc::new(RwLock::new(Consumers {
            consumers: vec![make_consumer("127.0.0.1:6379", true)],
        }));
        let cancel_token = Arc::new(CancellationToken::new());
        let (tx, rx) = mpsc::unbounded_channel::<ParsedBatch>();
        let (observer_tx, mut observer_rx) = mpsc::unbounded_channel::<DeliveryEvent>();

        let demux = start_demultiplexer_with_options(
            cancel_token.clone(),
            rx,
            consumers,
            options(),
            Some(observer_tx),
        )
        .await
        .unwrap();

        cancel_token.cancel();
        demux.wait().await;

        assert!(tx.send(ParsedBatch::Str(vec!["late".to_string()])).is_err());
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(150), observer_rx.recv()).await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn exposes_runtime_metrics() {
        let consumers = Arc::new(RwLock::new(Consumers {
            consumers: vec![
                make_consumer("127.0.0.1:6379", true),
                make_consumer("127.0.0.1:6380", true),
            ],
        }));
        let cancel_token = Arc::new(CancellationToken::new());
        let (tx, rx) = mpsc::unbounded_channel::<ParsedBatch>();

        let demux =
            start_demultiplexer_with_options(cancel_token.clone(), rx, consumers, options(), None)
                .await
                .unwrap();

        let metrics = demux.metrics();
        tokio::time::timeout(TEST_TIMEOUT, async {
            while metrics.active_workers() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("consumer workers did not start");

        tx.send(ParsedBatch::Str(vec!["one".to_string()])).unwrap();

        tokio::time::timeout(TEST_TIMEOUT, async {
            while metrics.deliveries() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delivery metrics were not updated");
        assert_eq!(metrics.input_batches(), 1);

        cancel_token.cancel();
        drop(tx);
        demux.wait().await;
    }
}
