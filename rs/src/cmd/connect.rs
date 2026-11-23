use std::{
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
};

use usage_rs::{Args, RunAsync};

use crate::connect::connect_ows;

/// Connect to ows websocket server and forward to tcp server
#[derive(Args)]
pub(crate) struct Connect {
    /// Address where the ows websocket server is listening
    #[usage()]
    from: String,

    /// Forwarded tcp listen address
    #[usage()]
    to: Option<String>,
}

impl Connect {
    //     fn from(&self) -> Result<SocketAddr, Box<dyn Error>> {
    //         let mut from_addrs = self
    //             .from
    //             .to_socket_addrs()
    //             .map_err(|e| format!("failed parse address '{}': {e}", self.from))?;
    //
    //         let from = from_addrs
    //             .next()
    //             .ok_or("cannot parse from address".to_string())?;
    //
    //         Ok(from)
    //     }

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

impl RunAsync for Connect {
    type Output = Result<(), Box<dyn Error>>;

    async fn run_async(self) -> Self::Output {
        let to = self.to()?;

        println!("listening {} and forwarding to {}", to, self.from);
        connect_ows(self.from, to).await?;

        Ok(())
    }
}
