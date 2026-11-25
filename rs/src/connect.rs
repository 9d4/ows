use std::{error::Error, net::SocketAddr, sync::Arc};

use futures_util::{
    SinkExt, StreamExt, TryStreamExt, future,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};

pub async fn connect_ows(addr: String, listen_addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    let ln = TcpListener::bind(listen_addr).await?;

    let peer_addr = Arc::new(addr);

    while let Ok((stream, _)) = ln.accept().await {
        let p = Arc::clone(&peer_addr);
        tokio::spawn(async move { accept_connection(stream, p).await });
    }

    Ok(())
}

async fn accept_connection(stream: TcpStream, peer_address: Arc<String>) {
    let remote_addr = stream
        .peer_addr()
        .expect("connected stream has no remote addr");

    println!("connected {}", remote_addr);

    let ws_req = peer_address
        .as_ref()
        .into_client_request()
        .expect("failed to create websocket request");
    let (ws_stream, _) = tokio_tungstenite::connect_async(ws_req)
        .await
        .expect("websocket handshake error");

    let (reader, writer) = stream.into_split();
    let (ws_writer, ws_reader) = ws_stream.split();
    let token = tokio_util::sync::CancellationToken::new();
    let t1 = token.clone();
    let t2 = token.clone();

    tokio::spawn(async move {
        t1.run_until_cancelled(copy_tcp_to_ws(reader, ws_writer))
            .await;
    });

    tokio::spawn(async move {
        t2.run_until_cancelled(copy_ws_to_tcp(ws_reader, writer))
            .await;
    });

    token.cancelled().await;
}

async fn copy_tcp_to_ws(
    mut tcp_reader: OwnedReadHalf,
    mut ws_writer: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        Message,
    >,
) {
    let mut buf = [0u8; 1024];
    loop {
        let n = match tcp_reader.read(&mut buf).await {
            Ok(n) if n == 0 => {
                println!("tcp closed");
                break;
            }
            Ok(n) => n,
            Err(e) => {
                println!("tcp read error: {}", e);
                break;
            }
        };

        let msg = Message::binary(buf[..n].to_vec());
        if let Err(e) = ws_writer.send(msg).await {
            println!("ws write error: {}", e);
            break;
        }
    }

    println!("tcp -> ws done");
}

async fn copy_ws_to_tcp(
    mut ws_reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    mut tcp_writer: OwnedWriteHalf,
) {
    let mut read = ws_reader
        .by_ref()
        .try_filter(|m| future::ready(m.is_binary()));

    while let Some(m) = read.next().await {
        let msg = match m {
            Ok(m) => m,
            Err(_) => {
                break;
            }
        };

        let mut buf = msg.into_data();
        let _ = tcp_writer.write_buf(&mut buf).await;
    }
}
