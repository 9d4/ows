use std::{
    io::{ErrorKind, Read},
    net::{TcpListener, TcpStream},
};

fn main() {
    let addr = "localhost:1212";
    let ln_res = TcpListener::bind(addr);

    let ln = match ln_res {
        Ok(val) => val,
        Err(e) => panic!("error listening tcp: {}", e),
    };

    println!("listening on {}", addr);

    loop {
        let accept_res = ln.accept();
        let (mut stream, addr) = match accept_res {
            Ok(res) => res,
            Err(err) => match err.kind() {
                ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::ConnectionRefused => continue,
                _ => panic!("error accepting connection: {}", err),
            },
        };

        println!("handling one connection from {}", addr.to_string());
        handle_stream(&mut stream);
    }
}

fn handle_stream(s: &mut TcpStream) {
    'read: loop {
        let mut buf = [0u8; 1024];

        match s.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                println!("read {}bytes", n);
            }
            Err(err) => {
                println!("error reading: {}", err);
                break 'read;
            }
        };

        println!("<< {}", String::from_utf8_lossy(&buf));
    }
}
