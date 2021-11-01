mod config;
mod http_api;
mod queue;

use crate::config::QueueHubType;
use crate::http_api::start_http;
use crate::queue::in_memory::InMemoryQueueHub;
use async_std::{io::Result as IoResult, task};

async fn setup() -> IoResult<()> {
    let cfg = config::read_config();

    tide::log::with_level(cfg.log_level);

    let queue_hub = match cfg.queue_hub_type {
        QueueHubType::InMemory => InMemoryQueueHub::new(cfg.max_queue_size),
    };

    start_http(queue_hub.clone(), cfg.http_sock_address).await
}

fn main() -> IoResult<()> {
    let server = setup();
    task::block_on(server)
}
