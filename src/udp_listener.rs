use std::sync::Arc;

use tokio::{net::UdpSocket, sync::mpsc::UnboundedSender};
use tokio_util::sync::CancellationToken;

use crate::{MultiplexerError, conf::Config};

pub fn start_udp_listener(
    conf: Arc<Config>,
    cancel_token: Arc<CancellationToken>,
    tx: UnboundedSender<Arc<[u8]>>,
    port: usize,
) -> Result<(), MultiplexerError> {
    let sock = Arc::new(
        std::net::UdpSocket::bind(format!("127.0.0.1:{port}"))
            .map_err(|err| MultiplexerError::InitUdpListener(err.to_string()))?,
    );

    for _ in 0..conf.conn_workers {
        let sock = sock
            .try_clone()
            .map_err(|err| MultiplexerError::InitUdpListener(err.to_string()))?;
        let cancel_token = cancel_token.clone();
        let conf = conf.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let sock = UdpSocket::from_std(sock).unwrap();
            let mut buf = vec![0u8; conf.udp_buffer_size];

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    result = sock.recv_from(&mut buf) => {
                        match result {
                            Ok((len, addr)) => {
                                println!("{:?} bytes received from {:?}", len, addr);
                                let data: Arc<[u8]> = Arc::from(&buf[..len]);
                                let _ = tx.send(data);
                            }
                            Err(err) => {
                                eprintln!("recv error: {}", err);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::start_udp_listener;
    use crate::conf::Config;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    fn make_conf(workers: usize) -> Arc<Config> {
        Arc::new(Config {
            conn_workers: workers,
            udp_buffer_size: 1024,
            port: 5402,
        })
    }

    #[tokio::test]
    async fn test_receives_packet() {
        let conf = make_conf(1);
        let cancel = Arc::new(CancellationToken::new());
        let (tx, mut rx) = mpsc::unbounded_channel::<Arc<[u8]>>();

        start_udp_listener(conf, cancel.clone(), tx, 15100).unwrap();

        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender.send_to(b"hello", "127.0.0.1:15100").await.unwrap();

        let data = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .unwrap_or(Arc::default());

        assert_eq!(&*data, b"hello");
        cancel.cancel();
    }

    #[tokio::test]
    async fn test_cancel_stops_worker() {
        let conf = make_conf(1);
        let cancel = Arc::new(CancellationToken::new());
        let (tx, _rx) = mpsc::unbounded_channel::<Arc<[u8]>>();

        start_udp_listener(conf, cancel.clone(), tx, 15101).unwrap();
        cancel.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_multiple_packets() {
        let conf = make_conf(1);
        let cancel = Arc::new(CancellationToken::new());
        let (tx, mut rx) = mpsc::unbounded_channel::<Arc<[u8]>>();

        start_udp_listener(conf, cancel.clone(), tx, 15102).unwrap();

        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for i in 0u8..5 {
            sender.send_to(&[i; 64], "127.0.0.1:15102").await.unwrap();
        }

        for i in 0u8..5 {
            let data = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .expect("timeout")
                .unwrap();
            assert_eq!(data[0], i);
        }

        cancel.cancel();
    }
}
