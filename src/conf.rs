use std::fmt::{Debug, Display};
use tokio::fs;

#[derive(Debug, serde::Deserialize, PartialEq)]
pub enum ParsedBatch {
    Str(Vec<String>),
    Json(Vec<serde_json::Value>),
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
pub enum ParseType {
    Str,
    Json,
}

#[derive(serde::Deserialize)]
pub struct Config {
    #[serde(default = "default_udp_buffer_size")]
    pub udp_buffer_size: usize,
    #[serde(default = "default_conn_workers")]
    pub conn_workers: usize,
    #[serde(default = "default_port")]
    pub port: usize,
    #[serde(default = "default_parse_batch_thresh")]
    pub parse_batch_thresh: usize,
    pub parse_type: ParseType,
}

fn default_udp_buffer_size() -> usize { 1024 }
fn default_conn_workers() -> usize { 4 }
fn default_port() -> usize { 5401 }
fn default_parse_batch_thresh() -> usize { 1000 }

impl Config {
    pub async fn load() -> Result<Self, ConfError> {
        let content = fs::read_to_string("config.toml")
            .await
            .map_err(|e| ConfError::LoadError(e.to_string()))?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| ConfError::ParseError(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfError> {
        if self.conn_workers == 0 {
            return Err(ConfError::ValidateError(
                "conn_workers must be at least 1".to_string(),
            ));
        }
        if self.parse_batch_thresh == 0 {
            return Err(ConfError::ValidateError(
                "parse_batch_thresh must be at least 1".to_string(),
            ));
        }
        if self.udp_buffer_size == 0 {
            return Err(ConfError::ValidateError(
                "udp_buffer_size must be greater than 0".to_string(),
            ));
        }
        if self.port == 0 || self.port > 65535 {
            return Err(ConfError::ValidateError(format!(
                "port {} is invalid, must be between 1 and 65535",
                self.port
            )));
        }
        Ok(())
    }
}

pub enum ConfError {
    LoadError(String),
    ParseError(String),
    ValidateError(String),
}

impl Display for ConfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfError::LoadError(e) => write!(f, "failed to load config: {}", e),
            ConfError::ParseError(e) => write!(f, "failed to parse config: {}", e),
            ConfError::ValidateError(e) => write!(f, "invalid config: {}", e),
        }
    }
}

impl Debug for ConfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}
