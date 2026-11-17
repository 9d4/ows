use std::error::Error;

use futures_util::{StreamExt, TryStreamExt, future};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

pub async fn listen_websocket<A: ToSocketAddrs>(
    addr: A,
    _forward_to: A,
) -> Result<(), Box<dyn Error>> {
    let ln = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("bind websocket: {}", e))?;

    while let Ok((stream, _)) = ln.accept().await {
        tokio::spawn(accept_connection(stream));
    }

    Ok(())
}

async fn accept_connection(stream: TcpStream) {
    let remote_addr = stream
        .peer_addr()
        .expect("connected stream has no remote addr");
    println!("connected {}", remote_addr);

    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("websocket handshake error");

    let (writer, reader) = ws_stream.split();

    reader
        .try_filter(|msg| future::ready(msg.is_text() || msg.is_binary()))
        .forward(writer)
        .await
        .expect("failed forwarding data")
}
