mod http_api;
mod queue;

use crate::http_api::start_http;
use crate::queue::QueueHub;
use async_std::{io::Result as IoResult, task};
use dotenv;
use std::env;
use tide::log::LevelFilter;

async fn setup() -> IoResult<()> {
    let log_level = env::var("LOG_LEVEL").unwrap_or("info".to_owned());
    let log_level: LevelFilter = log_level
        .parse()
        .expect(&format!("incorrect log level: {}", log_level));
    tide::log::with_level(log_level);

    let max_queue_size = env::var("MAX_QUEUE_SIZE")
        .unwrap_or("10000".to_owned())
        .parse()
        .expect("max queue size must be a positive number");
    let queue_hub = QueueHub::new(max_queue_size);

    let http_sock_addr =
        env::var("HTTP_SOCKET_ADDRESS").unwrap_or("127.0.0.1:8822".to_owned());
    start_http(queue_hub.clone(), &http_sock_addr).await
}

fn main() -> IoResult<()> {
    dotenv::from_filename(".env").ok();
    let server = setup();
    task::block_on(server)
}
