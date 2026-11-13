use std::{
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
};

use usage_rs::{Args, Run};

/// Serve any tcp server as websocket
#[derive(Args)]
pub(crate) struct Serve {
    /// Address where packet will be forwarded to
    #[usage()]
    from: String,

    /// Websocket listen address
    #[usage()]
    to: String,
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
        let mut addrs = self
            .to
            .to_socket_addrs()
            .map_err(|e| format!("failed parse address '{}': {e}", self.to))?;

        let addr = addrs
            .next()
            .ok_or("cannot parse listen address".to_string())?;

        Ok(addr)
    }
}

impl Run for Serve {
    type Output = Result<(), Box<dyn Error>>;

    fn run(self) -> Self::Output {
        let from = self.from()?;
        println!("forward to {from}");

        if !self.to.is_empty() {
            let to = self.to()?;
            println!("listen on {to}");
        }

        Ok(())
    }
}
