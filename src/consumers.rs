use notify::{Event, RecursiveMode, Watcher};
use serde::Deserialize;
use std::{fs, path::Path, sync::Arc, time::Duration};

use tokio::sync::RwLock;

use crate::MultiplexerError;

#[derive(Debug, Deserialize, Clone)]
pub struct Consumers {
    pub consumers: Vec<Consumer>,
}
#[derive(Debug, Deserialize, Clone)]
pub enum ConsumerType {
    Elastic,
    Redis,
}
#[derive(Debug, Deserialize, Clone)]
pub struct Consumer {
    pub consumer_type: ConsumerType,
    pub addr: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub target_name: Option<String>, // can be redis list or elastic index prefix
    #[serde(default = "default_consumer_enable")]
    pub enable: bool,
}

fn default_consumer_enable() -> bool {
    true
}

impl Consumers {
    fn load() -> Result<Self, MultiplexerError> {
        let s = fs::read_to_string("consumers.toml").map_err(|err| {
            MultiplexerError::LoadConsumers(format!(
                "failed to read consumers.toml: {}",
                err.to_string()
            ))
        })?;
        Ok(toml::from_str(&s).map_err(|err| {
            MultiplexerError::LoadConsumers(format!(
                "failed to convert consumers.toml to string: {}",
                err.to_string()
            ))
        })?)
    }

    pub fn load_shared() -> Result<Arc<RwLock<Self>>, MultiplexerError> {
        Ok(Arc::new(RwLock::new(Self::load()?)))
    }

    pub async fn start_hot_reload(shared: Arc<RwLock<Self>>) -> Result<(), MultiplexerError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut watcher = notify::recommended_watcher(move |res: Result<Event, _>| {
            if let Ok(e) = res {
                if e.kind.is_modify() {
                    let _ = tx.blocking_send(());
                }
            }
        })
        .map_err(|err| {
            MultiplexerError::LoadConsumers(format!(
                "failed to start consumers.toml watcher: {}",
                err.to_string()
            ))
        })?;
        watcher
            .watch(Path::new("consumers.toml"), RecursiveMode::NonRecursive)
            .map_err(|err| {
                MultiplexerError::LoadConsumers(format!(
                    "failed to start consumers.toml watcher: {}",
                    err.to_string()
                ))
            })?;

        tokio::spawn(async move {
            let _watcher = watcher;
            while rx.recv().await.is_some() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                match Self::load() {
                    Ok(cfg) => {
                        *shared.write().await = cfg;
                    }
                    Err(_) => println!("failed to reload consumers"),
                }
            }
        });
        Ok(())
    }
}
