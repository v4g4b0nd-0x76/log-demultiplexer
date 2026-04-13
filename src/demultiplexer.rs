use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use bb8_redis::redis;
use elasticsearch::{
    Elasticsearch,
    auth::Credentials,
    cert::CertificateValidation,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
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
    consumers::{Consumer, ConsumerType, Consumers},
};

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const HEALTHCHECK_INTERVAL: Duration = Duration::from_secs(5);
const HEALTHCHECK_BACKOFF: Duration = Duration::from_secs(30);
const REDIS_CHUNK_SIZE: usize = 10_000;
const REDIS_POOL_SIZE: u32 = 8;





#[derive(Default)]
pub struct DemultiplexerMetrics {
    input_batches: AtomicU64,
    deliveries: AtomicU64,
    active_workers: AtomicUsize,
}

impl DemultiplexerMetrics {
    pub fn input_batches(&self) -> u64 { self.input_batches.load(Ordering::Relaxed) }
    pub fn deliveries(&self) -> u64 { self.deliveries.load(Ordering::Relaxed) }
    pub fn active_workers(&self) -> usize { self.active_workers.load(Ordering::Relaxed) }
}





enum ConsumerConn {
    Redis(Pool<RedisConnectionManager>),
    Elastic(Elasticsearch),
}

impl ConsumerConn {
    async fn build(consumer: &Consumer) -> Result<Self, MultiplexerError> {
        match consumer.consumer_type {
            ConsumerType::Redis => {
                let url = redis_url(consumer);
                let manager = RedisConnectionManager::new(url)
                    .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;
                let pool = Pool::builder()
                    .max_size(REDIS_POOL_SIZE)
                    .connection_timeout(Duration::from_secs(2))
                    .build(manager)
                    .await
                    .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;
                Ok(Self::Redis(pool))
            }
            ConsumerType::Elastic => {
                let client = build_elastic_client(consumer)?;
                Ok(Self::Elastic(client))
            }
        }
    }
}





pub struct Demultiplexer {
    manager: JoinHandle<()>,
    metrics: Arc<DemultiplexerMetrics>,
}

impl Demultiplexer {
    pub async fn wait(self) { let _ = self.manager.await; }
    pub fn metrics(&self) -> Arc<DemultiplexerMetrics> { Arc::clone(&self.metrics) }
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
        run_manager(cancel_token, rx, consumers, options, observer, manager_metrics).await;
    });

    Ok(Demultiplexer { manager, metrics })
}





async fn run_manager(
    cancel_token: Arc<CancellationToken>,
    mut rx: UnboundedReceiver<ParsedBatch>,
    consumers: Arc<RwLock<Consumers>>,
    options: RuntimeOptions,
    observer: Option<UnboundedSender<DeliveryEvent>>,
    metrics: Arc<DemultiplexerMetrics>,
) {
    let mut runtimes = HashMap::<String, ConsumerRuntime>::new();
    let mut reconcile_tick = tokio::time::interval(options.reconcile_interval);
    reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let snapshot = consumers.read().await.clone();
    reconcile_consumers(&snapshot, &cancel_token, &mut runtimes, &metrics, options, observer.clone()).await;

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => break,
            _ = reconcile_tick.tick() => {
                let snapshot = consumers.read().await.clone();
                reconcile_consumers(&snapshot, &cancel_token, &mut runtimes, &metrics, options, observer.clone()).await;
            }
            batch = rx.recv() => {
                let Some(batch) = batch else { break };
                metrics.input_batches.fetch_add(1, Ordering::Relaxed);
                let shared = Arc::new(batch);
                let dead = dispatch_batch(&runtimes, shared);
                for key in dead {
                    if let Some(rt) = runtimes.remove(&key) {
                        stop_runtime(rt, &metrics).await;
                    }
                }
            }
        }
    }

    for (_, rt) in runtimes.drain() {
        stop_runtime(rt, &metrics).await;
    }
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

    for consumer in consumers.consumers.iter().filter(|c| c.enable) {
        let key = consumer_key(consumer);
        desired.insert(key.clone());

        if runtimes.contains_key(&key) {
            continue;
        }

        match spawn_consumer_runtime(key.clone(), consumer.clone(), cancel_token, Arc::clone(metrics), options, observer.clone()).await {
            Ok(runtime) => {
                metrics.active_workers.fetch_add(1, Ordering::Relaxed);
                runtimes.insert(key, runtime);
            }
            Err(e) => {
                eprintln!("failed to start consumer {key}: {e}");
            }
        }
    }

    let stale: Vec<_> = runtimes.keys().filter(|k| !desired.contains(*k)).cloned().collect();
    for key in stale {
        if let Some(rt) = runtimes.remove(&key) {
            stop_runtime(rt, metrics).await;
        }
    }
}





async fn spawn_consumer_runtime(
    consumer_key: String,
    consumer: Consumer,
    cancel_token: &CancellationToken,
    metrics: Arc<DemultiplexerMetrics>,
    options: RuntimeOptions,
    observer: Option<UnboundedSender<DeliveryEvent>>,
) -> Result<ConsumerRuntime, MultiplexerError> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<ParsedBatch>>();
    let stop_token = cancel_token.child_token();

    
    let conn = Arc::new(ConsumerConn::build(&consumer).await?);
    let consumer = Arc::new(consumer);

    
    let worker = {
        let conn = Arc::clone(&conn);
        let consumer = Arc::clone(&consumer);
        let token = stop_token.clone();
        let key = consumer_key.clone();
        let metrics = Arc::clone(&metrics);
        let observer = observer.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    batch = rx.recv() => {
                        let Some(batch) = batch else { break };
                        metrics.deliveries.fetch_add(1, Ordering::Relaxed);
                        if let Some(obs) = observer.as_ref() {
                            let _ = obs.send(DeliveryEvent { consumer_key: key.clone(), batch: Arc::clone(&batch) });
                        }
                        let result = match conn.as_ref() {
                            ConsumerConn::Elastic(client) => index_elastic_batch(client, &consumer, batch).await,
                            ConsumerConn::Redis(pool) => push_redis_batch(pool, &consumer, batch).await,
                        };
                        if let Err(e) = result {
                            eprintln!("delivery error on {key}: {e}");
                        }
                    }
                }
            }
        })
    };

    
    let healthcheck = {
        let conn = Arc::clone(&conn);
        let consumer = Arc::clone(&consumer);
        let token = stop_token.clone();
        let key = consumer_key.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(options.healthcheck_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut consecutive_failures: u8 = 0;

            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = ticker.tick() => {
                        let healthy = match conn.as_ref() {
                            ConsumerConn::Elastic(client) => elastic_health_check(client).await.is_ok(),
                            ConsumerConn::Redis(pool) => redis_health_check(pool).await.is_ok(),
                        };

                        if healthy {
                            consecutive_failures = 0;
                            continue;
                        }

                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures > 3 {
                            eprintln!(
                                "consumer {key} ({:?}) failed 3 health checks; backing off {:?}",
                                consumer.consumer_type, options.healthcheck_backoff
                            );
                            tokio::time::sleep(options.healthcheck_backoff).await;
                            consecutive_failures = 0;
                        }
                    }
                }
            }
        })
    };

    Ok(ConsumerRuntime { tx, stop_token, worker, healthcheck })
}

async fn stop_runtime(runtime: ConsumerRuntime, metrics: &DemultiplexerMetrics) {
    runtime.stop_token.cancel();
    drop(runtime.tx);
    let _ = runtime.worker.await;
    let _ = runtime.healthcheck.await;
    metrics.active_workers.fetch_sub(1, Ordering::Relaxed);
}





fn dispatch_batch(runtimes: &HashMap<String, ConsumerRuntime>, batch: Arc<ParsedBatch>) -> Vec<String> {
    let mut dead = Vec::new();
    for (key, rt) in runtimes {
        if rt.tx.send(Arc::clone(&batch)).is_err() {
            dead.push(key.clone());
        }
    }
    dead
}





async fn index_elastic_batch(
    client: &Elasticsearch,
    consumer: &Consumer,
    batch: Arc<ParsedBatch>,
) -> Result<(), MultiplexerError> {
    let index = target_list_name(consumer)?;

    let body: Vec<elasticsearch::BulkOperation<serde_json::Value>> = match batch.as_ref() {
        ParsedBatch::Str(items) => items.iter().map(|s| {
            let doc = serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!({ "raw": s }));
            elasticsearch::BulkOperation::index(doc).into()
        }).collect(),
        ParsedBatch::Json(items) => items.iter()
            .map(|v| elasticsearch::BulkOperation::index(v.clone()).into())
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
    pool: &Pool<RedisConnectionManager>,
    consumer: &Consumer,
    batch: Arc<ParsedBatch>,
) -> Result<(), MultiplexerError> {
    let list = target_list_name(consumer)?;

    let values: Vec<Vec<u8>> = match batch.as_ref() {
        ParsedBatch::Str(items) => items.iter().map(|s| s.as_bytes().to_vec()).collect(),
        ParsedBatch::Json(items) => items.iter().map(|v| v.to_string().into_bytes()).collect(),
    };

    let tasks: Vec<_> = values
        .chunks(REDIS_CHUNK_SIZE)
        .map(|chunk| {
            let pool = pool.clone();
            let list = list.clone();
            let chunk = chunk.to_vec();
            tokio::spawn(async move {
                let mut conn = pool.get().await
                    .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;
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





async fn elastic_health_check(client: &Elasticsearch) -> Result<(), MultiplexerError> {
    let res = client.ping().send().await
        .map_err(|e| MultiplexerError::ElasticClient(e.to_string()))?;

    match res.status_code().as_u16() {
        200 | 403 => Ok(()),
        code => Err(MultiplexerError::ElasticClient(format!("unexpected ping status: {code}"))),
    }
}

async fn redis_health_check(pool: &Pool<RedisConnectionManager>) -> Result<(), MultiplexerError> {
    let mut conn = tokio::time::timeout(Duration::from_secs(2), pool.get())
        .await
        .map_err(|_| MultiplexerError::RedisClient("pool acquire timed out".into()))?
        .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;

    let pong: String = tokio::time::timeout(
        Duration::from_secs(2),
        redis::cmd("PING").query_async(&mut *conn),
    )
    .await
    .map_err(|_| MultiplexerError::RedisClient("ping timed out".into()))?
    .map_err(|e| MultiplexerError::RedisClient(e.to_string()))?;

    if pong == "PONG" { Ok(()) } else { Err(MultiplexerError::RedisClient(format!("unexpected PING reply: {pong}"))) }
}





fn build_elastic_client(consumer: &Consumer) -> Result<Elasticsearch, MultiplexerError> {
    let url = Url::parse(&consumer.addr)
        .map_err(|e| MultiplexerError::ElasticClient(e.to_string()))?;

    let builder = TransportBuilder::new(SingleNodeConnectionPool::new(url))
        .cert_validation(CertificateValidation::None);

    let transport = match (&consumer.username, &consumer.password) {
        (Some(user), Some(pass)) => builder.auth(Credentials::Basic(user.clone(), pass.clone())),
        _ => builder,
    }
    .build()
    .map_err(|e| MultiplexerError::ElasticClient(e.to_string()))?;

    Ok(Elasticsearch::new(transport))
}

fn redis_url(consumer: &Consumer) -> String {
    match &consumer.password {
        Some(pass) => format!("redis:
        None => format!("redis:
    }
}

fn target_list_name(consumer: &Consumer) -> Result<String, MultiplexerError> {
    match &consumer.target_name {
        Some(name) => Ok(format!("{}-{}", name, chrono::Utc::now().format("%Y.%m.%d"))),
        None => Err(MultiplexerError::ElasticClient("no target_name configured".into())),
    }
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





#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};
    use tokio::sync::{RwLock, mpsc::{self, UnboundedReceiver}};
    use tokio_util::sync::CancellationToken;
    use super::{DeliveryEvent, RuntimeOptions, consumer_key, start_demultiplexer_with_options};
    use crate::{conf::ParsedBatch, consumers::{Consumer, ConsumerType, Consumers}};

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
            .expect("timed out waiting for event")
            .expect("observer channel closed")
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
        let (obs_tx, mut obs_rx) = mpsc::unbounded_channel::<DeliveryEvent>();

        let demux = start_demultiplexer_with_options(cancel_token.clone(), rx, consumers, options(), Some(obs_tx))
            .await.unwrap();

        tx.send(ParsedBatch::Str(vec!["alpha".to_string()])).unwrap();

        let a = recv_event(&mut obs_rx).await;
        let b = recv_event(&mut obs_rx).await;
        assert_ne!(a.consumer_key, b.consumer_key);
        assert!(Arc::ptr_eq(&a.batch, &b.batch));

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
        let consumers = Arc::new(RwLock::new(Consumers { consumers: vec![first, second] }));
        let cancel_token = Arc::new(CancellationToken::new());
        let (tx, rx) = mpsc::unbounded_channel::<ParsedBatch>();
        let (obs_tx, mut obs_rx) = mpsc::unbounded_channel::<DeliveryEvent>();

        let demux = start_demultiplexer_with_options(cancel_token.clone(), rx, consumers.clone(), options(), Some(obs_tx))
            .await.unwrap();

        tx.send(ParsedBatch::Str(vec!["phase1".to_string()])).unwrap();
        let mut p1 = vec![recv_event(&mut obs_rx).await, recv_event(&mut obs_rx).await];
        p1.sort_by(|a, b| a.consumer_key.cmp(&b.consumer_key));
        assert_eq!(p1[0].consumer_key, first_key);
        assert_eq!(p1[1].consumer_key, second_key);

        consumers.write().await.consumers[1].enable = false;
        tokio::time::sleep(TEST_RECONCILE * 2).await;

        tx.send(ParsedBatch::Str(vec!["phase2".to_string()])).unwrap();
        let p2 = recv_event(&mut obs_rx).await;
        assert_eq!(p2.consumer_key, first_key);
        assert!(tokio::time::timeout(Duration::from_millis(150), obs_rx.recv()).await.is_err());

        consumers.write().await.consumers[1].enable = true;
        tokio::time::sleep(TEST_RECONCILE * 2).await;

        tx.send(ParsedBatch::Str(vec!["phase3".to_string()])).unwrap();
        let mut p3 = vec![recv_event(&mut obs_rx).await, recv_event(&mut obs_rx).await];
        p3.sort_by(|a, b| a.consumer_key.cmp(&b.consumer_key));
        assert_eq!(p3[0].consumer_key, first_key);
        assert_eq!(p3[1].consumer_key, second_key);

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
        let (obs_tx, mut obs_rx) = mpsc::unbounded_channel::<DeliveryEvent>();

        let demux = start_demultiplexer_with_options(cancel_token.clone(), rx, consumers, options(), Some(obs_tx))
            .await.unwrap();

        cancel_token.cancel();
        demux.wait().await;

        assert!(tx.send(ParsedBatch::Str(vec!["late".to_string()])).is_err());
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(150), obs_rx.recv()).await,
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

        let demux = start_demultiplexer_with_options(cancel_token.clone(), rx, consumers, options(), None)
            .await.unwrap();

        let metrics = demux.metrics();
        tokio::time::timeout(TEST_TIMEOUT, async {
            while metrics.active_workers() != 2 { tokio::task::yield_now().await; }
        }).await.expect("workers did not start");

        tx.send(ParsedBatch::Str(vec!["one".to_string()])).unwrap();
        tokio::time::timeout(TEST_TIMEOUT, async {
            while metrics.deliveries() < 2 { tokio::task::yield_now().await; }
        }).await.expect("deliveries not counted");

        assert_eq!(metrics.input_batches(), 1);
        cancel_token.cancel();
        drop(tx);
        demux.wait().await;
    }
}
