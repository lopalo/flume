mod http_api;
mod queue;

use crate::http_api::start_http;
use async_std::{io::Result as IoResult, task};

async fn setup() -> IoResult<()> {
    start_http().await
}

fn main() -> IoResult<()> {
    let server = setup();
    task::block_on(server)
}
