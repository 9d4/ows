use tokio::io::{AsyncBufReadExt, BufReader, ErrorKind};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = "localhost:1212";
    let ln_res = TcpListener::bind(addr).await;

    let ln = match ln_res {
        Ok(val) => val,
        Err(e) => panic!("error listening tcp: {}", e),
    };

    println!("listening on {}", addr);

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
