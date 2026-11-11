use usage_rs::{Args, Run};

/// Serve any tcp server as websocket
#[derive(Args)]
pub(crate) struct Serve {
    /// Address where packet will be forwarded to
    from: String,
}

impl Run for Serve {
    type Output = ();

    fn run(self) -> Self::Output {
        todo!()
    }
}
