use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use log_demultiplexer::MultiplexerError;
use log_demultiplexer::udp_listener::start_udp_listener;

#[tokio::main]
async fn main() -> Result<(), MultiplexerError> {
    let conf = Arc::new(
        log_demultiplexer::conf::Config::load()
            .await
            .map_err(MultiplexerError::LoadConfig)?,
    );
    let cancel_token = Arc::new(CancellationToken::new());

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Arc<[u8]>>();
    start_udp_listener(
        Arc::clone(&conf),
        Arc::clone(&cancel_token),
        tx,
        conf.as_ref().port,
    )?;

    // multi worker connection acceptor
    // sharded raw fixed size manually allocated ring buffer
    // parser which parse data into shared pointer
    // demultiplexer

    wait_for_shutdown().await;
    cancel_token.cancel();

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
