use std::{error::Error, net::ToSocketAddrs};

use usage_rs::{Args, Run};

/// Serve any tcp server as websocket
#[derive(Args)]
pub(crate) struct Serve {
    /// Address where packet will be forwarded to
    #[usage()]
    from: String,
}

impl Run for Serve {
    type Output = Result<(), Box<dyn Error>>;

    fn run(self) -> Self::Output {
        let mut from_addrs = self
            .from
            .to_socket_addrs()
            .map_err(|e| format!("failed parse address '{}': {e}", self.from))?;

        let from = from_addrs
            .next()
            .ok_or("cannot parse from address".to_string())?;

        println!("addr {from}");
        Ok(())
    }
}
