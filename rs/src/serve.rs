use std::{error::Error, net::SocketAddr, sync::Arc};

use futures_util::{SinkExt, StreamExt, TryStreamExt, future};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_util::{future::FutureExt, sync::CancellationToken};

pub async fn listen_websocket(
    addr: SocketAddr,
    forward_to: SocketAddr,
) -> Result<(), Box<dyn Error>> {
    let ln = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("bind websocket: {}", e))?;

    let peer_addr = Arc::new(forward_to);

    while let Ok((stream, _)) = ln.accept().await {
        let p = Arc::clone(&peer_addr);
        tokio::spawn(async move { accept_connection(stream, *p).await });
    }

    Ok(())
}

async fn accept_connection(stream: TcpStream, peer_address: SocketAddr) {
    let remote_addr = stream
        .peer_addr()
        .expect("connected stream has no remote addr");

    println!("connected {}", remote_addr);
    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("websocket handshake error");
    let (mut writer, reader) = ws_stream.split();

    let tcp_stream = match TcpStream::connect(peer_address).await {
        Ok(s) => s,
        Err(err) => {
            println!("[{}] connect peer error: {}", remote_addr, err);
            return;
        }
    };
    let (mut tcp_reader, mut tcp_writer) = tcp_stream.into_split();

    let token = CancellationToken::new();
    let tcp_token = token.clone();
    let ws_token = token.clone();

    tokio::spawn(async move {
        tcp_token
            .run_until_cancelled(async {
                let mut data =
                    reader.try_filter(|msg| future::ready(msg.is_text() || msg.is_binary()));

                while let Some(msg) = data.next().await {
                    println!("ws got data");
                    let data = match msg {
                        Ok(m) => m,
                        Err(_) => break,
                    };

                    let mut buf = data.into_data();
                    let _ = tcp_writer.write_buf(&mut buf).await;
                }

                println!("tcp forward is canceling");
                tcp_token.cancel();
            })
            .await;

        println!("tcp forward done");
    });

    tokio::spawn(async move {
        ws_token
            .run_until_cancelled(async {
                let mut buf = [0u8; 8192];
                loop {
                    let n = tcp_reader
                        .read(&mut buf)
                        .await
                        .expect("peer error reading data");

                    if n == 0 {
                        break;
                    };

                    let _ = writer
                        .send(Message::Binary(Bytes::copy_from_slice(&buf[..n])))
                        .await;
                }

                println!("ws forward is canceling");
                ws_token.cancel();
            })
            .await;

        println!("ws forward done");
    });

    token.cancelled().await;
    println!("done");
}
