use std::{
    error::Error,
    io::ErrorKind,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use usage_rs::{Args, RunAsync};

use crate::serve::listen_websocket;

/// Serve any tcp server as websocket
#[derive(Args)]
pub(crate) struct Serve {
    /// Address where packet will be forwarded to
    #[usage()]
    from: String,

    /// Websocket listen address
    #[usage()]
    to: Option<String>,
}

impl Serve {
    fn from(&self) -> Result<SocketAddr, Box<dyn Error>> {
        let mut from_addrs = self
            .from
            .to_socket_addrs()
            .map_err(|e| format!("failed parse address '{}': {e}", self.from))?;

        let from = from_addrs
            .next()
            .ok_or("cannot parse from address".to_string())?;

        Ok(from)
    }

    fn to(&self) -> Result<SocketAddr, Box<dyn Error>> {
        let to = self.to.as_deref().unwrap_or("0.0.0.0:0");

        let mut addrs = to
            .to_socket_addrs()
            .map_err(|e| format!("failed parse address '{}': {e}", to))?;

        Ok(addrs
            .next()
            .ok_or("cannot parse listen address".to_string())?)
    }
}

impl RunAsync for Serve {
    type Output = Result<(), Box<dyn Error>>;

    async fn run_async(self) -> Self::Output {
        let from = Arc::new(self.from()?);
        println!("forwarding to {from}");

        let _ = listen_websocket(self.to()?, *from).await?;

        let ln = TcpListener::bind(self.to()?).await?;
        let listen_addr = ln.local_addr()?;
        println!("listening on {}", listen_addr.to_string());

        loop {
            let accept_res = ln.accept().await;
            let (stream, _) = match accept_res {
                Ok(res) => res,
                Err(err) => match err.kind() {
                    ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::ConnectionRefused => continue,
                    _ => panic!("error accepting connection: {}", err),
                },
            };

            let from = Arc::clone(&from);
            tokio::spawn(handle_stream(stream, from));
        }
    }
}

async fn handle_stream(s: TcpStream, peer: Arc<SocketAddr>) {
    let addr = match s.peer_addr() {
        Ok(v) => v.to_string(),
        Err(_) => String::from("unknown"),
    };

    println!("handling connection from {}", addr);
    println!("connecting to {}", peer);

    let upstream = match TcpStream::connect(peer.to_string()).await {
        Ok(s) => s,
        Err(err) => {
            eprint!("error connecting to {}: {}", peer.to_string(), err);
            return;
        }
    };

    let (upstream_reader, mut upstream_writer) = io::split(upstream);
    let (stream_reader, mut stream_writer) = io::split(s);

    tokio::spawn(async move {
        let mut r = BufReader::new(upstream_reader);
        loop {
            let b = match r.read_u8().await {
                Ok(b) => b,
                Err(err) => {
                    println!("error reading upstream: {}", err);
                    return;
                }
            };

            let _ = stream_writer.write_u8(b).await;
        }
    });

    tokio::spawn(async move {
        let mut r = BufReader::new(stream_reader);
        loop {
            let b = match r.read_u8().await {
                Ok(b) => b,
                Err(err) => {
                    println!("error reading upstream: {}", err);
                    return;
                }
            };

            let _ = upstream_writer.write_u8(b).await;
        }
    });

    println!("done {}", addr);
}
