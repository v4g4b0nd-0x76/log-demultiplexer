pub mod conf;
pub mod consumers;
pub mod demultiplexer;
pub mod parser;
pub mod persist;
pub mod udp_listener;
use std::fmt::{Debug, Display};

pub enum MultiplexerError {
    InitUdpListener(String),
    AcceptUdpConn(String),
    LoadConfig(conf::ConfError),
    SpawnWorker((usize, String)),
    LoadConsumers(String),
    ElasticClient(String),
    RedisClient(String),
}

impl Debug for MultiplexerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiplexerError::InitUdpListener(err) => {
                write!(f, "failed to initialize udp listener: {err}")
            }
            MultiplexerError::AcceptUdpConn(err) => {
                write!(f, "failed to accept udp connection: {err}")
            }
            MultiplexerError::LoadConfig(conf_error) => write!(f, "{:#?}", conf_error),
            MultiplexerError::SpawnWorker((worker, err)) => {
                write!(f, "failed to spawn worker {}: {}", worker, err)
            }
            MultiplexerError::LoadConsumers(err) => write!(f, "failed to load consumers: {}", err),
            MultiplexerError::ElasticClient(err) => write!(f, "elastic failed: {}", err),
            MultiplexerError::RedisClient(err) => write!(f, "redis failed: {}", err),
        }
    }
}

impl Display for MultiplexerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiplexerError::InitUdpListener(err) => {
                write!(f, "failed to initialize udp listener: {err}")
            }
            MultiplexerError::AcceptUdpConn(err) => {
                write!(f, "failed to accept udp connection: {err}")
            }
            MultiplexerError::LoadConfig(conf_error) => write!(f, "{:#?}", conf_error),
            MultiplexerError::SpawnWorker((worker, err)) => {
                write!(f, "failed to spawn worker {}: {}", worker, err)
            }
            MultiplexerError::LoadConsumers(err) => write!(f, "failed to load consumers: {}", err),
            MultiplexerError::ElasticClient(err) => write!(f, "elastic failed: {}", err),
            MultiplexerError::RedisClient(err) => write!(f, "redis failed: {}", err),
        }
    }
}
