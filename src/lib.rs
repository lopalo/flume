mod config;
mod http_api;
pub mod queue;

use crate::config::{Config, QueueHubType};
use crate::http_api::start_http;
#[cfg(feature = "sqlite")]
use crate::queue::sqlite::SqliteQueueHub;
use crate::queue::{in_memory::InMemoryQueueHub, QueueHub};
use anyhow::Result;
use async_std::task;
use std::{future::Future, pin::Pin, time::Duration};

async fn garbage_collector<QH>(queue_hub: QH, period: Duration) -> Result<()>
where
    QH: QueueHub,
{
    log::info!("Run garbage collection every {:?}", period);
    loop {
        task::sleep(period).await;
        log::info!("Garbage collection");
        if let Err(err) = queue_hub.collect_garbage().await {
            log::error!("GC failed: {}", err)
        }
    }
}

fn run_with<QH>(
    cfg: Config,
    queue_hub: QH,
) -> Pin<Box<dyn Future<Output = Result<()>>>>
where
    QH: QueueHub,
{
    Box::pin(async move {
        task::spawn(garbage_collector(
            queue_hub.clone(),
            cfg.garbage_collection_period,
        ));
        start_http(queue_hub.clone(), cfg.http_sock_address)
            .await
            .map_err(|e| e.into())
    })
}

pub async fn setup_server() -> Result<()> {
    let cfg = config::read_config();

    femme::with_level(cfg.log_level);

    match cfg.queue_hub_type {
        QueueHubType::InMemory => {
            let queue_hub = InMemoryQueueHub::new(cfg.max_queue_size);
            run_with(cfg, queue_hub)
        }
        #[cfg(feature = "sqlite")]
        QueueHubType::Sqlite => {
            let queue_hub =
                SqliteQueueHub::connect(cfg.database_url.clone()).await?;
            run_with(cfg, queue_hub)
        }
    }
    .await?;
    Ok(())
}
