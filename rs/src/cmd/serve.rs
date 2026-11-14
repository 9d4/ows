use std::{
    error::Error,
    io::ErrorKind,
    net::{SocketAddr, ToSocketAddrs},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
};
use usage_rs::{Args, RunAsync};

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
        let from = self.from()?;
        println!("forwarding to {from}");

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

            tokio::spawn(handle_stream(stream));
        }

        // Ok(())
    }
}

async fn handle_stream(mut s: TcpStream) {
    let addr = match s.peer_addr() {
        Ok(v) => v.to_string(),
        Err(_) => String::from("unknown"),
    };

    println!("handling connection from {}", addr);

    let mut lines = BufReader::new(&mut s).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                println!("[{}]<< {}", addr, line);
            }
            Ok(_) => break,
            Err(err) => {
                println!("error reading: {}", err);
                break;
            }
        }
    }

    println!("done {}", addr);
}
