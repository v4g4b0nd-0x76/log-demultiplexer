use std::sync::Arc;

use log_demultiplexer::conf::ParsedBatch;
use tokio_util::sync::CancellationToken;

use log_demultiplexer::MultiplexerError;
use log_demultiplexer::parser::start_parser;
use log_demultiplexer::udp_listener::start_udp_listener;
#[tokio::main]
async fn main() -> Result<(), MultiplexerError> {
    let conf = Arc::new(
        log_demultiplexer::conf::Config::load()
            .await
            .map_err(MultiplexerError::LoadConfig)?,
    );
    let cancel_token = Arc::new(CancellationToken::new());

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Arc<[u8]>>();
    // multi worker connection acceptor
    let listener = start_udp_listener(
        Arc::clone(&conf),
        Arc::clone(&cancel_token),
        tx,
        conf.as_ref().port,
    )?;

    let (d_tx, _d_rx) = tokio::sync::mpsc::unbounded_channel::<ParsedBatch>();
    // multi worker dynamic type parser
    let parser = start_parser(Arc::clone(&conf), rx, d_tx)?;
    // demultiplexer

    wait_for_shutdown().await;
    cancel_token.cancel();
    listener.wait().await;
    parser.wait().await;

    Ok(())
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.unwrap();
    }
}
