use std::fmt::Debug;

use tokio::fs;
#[derive(serde::Deserialize)]
pub struct Config {
    #[serde(default = "default_udp_buffer_size")]
    pub udp_buffer_size: usize,
    #[serde(default = "default_conn_workers")]
    pub conn_workers: usize,
    #[serde(default = "default_port")]
    pub port: usize,
}
fn default_udp_buffer_size() -> usize {
    1024
}
fn default_conn_workers() -> usize {
    4
}
fn default_port() -> usize {
    5401
}

impl Config {
    pub async fn load() -> Result<Self, ConfError> {
        let content = fs::read_to_string("config.toml")
            .await
            .map_err(|err| ConfError::LoadError(err.to_string()))?;
        let config: Config =
            toml::from_str(&content).map_err(|err| ConfError::ParseError(err.to_string()))?;
        Ok(config)
    }
}

pub enum ConfError {
    LoadError(String),
    ParseError(String),
}
impl Debug for ConfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfError::LoadError(err) => write!(f, "failed to load config: {}", err),
            ConfError::ParseError(err) => write!(f, "failed to parse config: {}", err),
        }
    }
}
