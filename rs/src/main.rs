use std::{
    io::{BufRead, BufReader, ErrorKind},
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
        let (stream, addr) = match accept_res {
            Ok(res) => res,
            Err(err) => match err.kind() {
                ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::ConnectionRefused => continue,
                _ => panic!("error accepting connection: {}", err),
            },
        };

        println!("handling one connection from {}", addr.to_string());
        handle_stream(stream);
    }
}

fn handle_stream(s: TcpStream) {
    let reader = BufReader::new(&s);

    for line in reader.lines() {
        match line {
            Ok(val) => {
                println!("<< {}", val);
            }
            Err(err) => {
                println!("error reading: {}", err);
                break;
            }
        };
    }

    println!("done");
}
