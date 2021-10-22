use async_std::task;
use std::io::Result as IoResult;

async fn start_http() -> IoResult<()> {
    tide::log::start();
    let mut app = tide::new();
    app.at("/").get(|_| async { Ok("This is Flume") });
    app.listen("127.0.0.1:8080").await?;
    Ok(())
}

fn main() -> IoResult<()> {
    task::block_on(start_http())
}
