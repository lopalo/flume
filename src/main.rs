use anyhow::Result;
use async_std::task;

fn main() -> Result<()> {
    let server = flume::setup_server();
    task::block_on(server)
}
