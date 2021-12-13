mod broadcaster;
mod config;
mod http_api;
pub mod queue;

#[cfg(feature = "sqlite")]
use crate::queue::sqlite::SqliteQueueHub;
use crate::{
    broadcaster::Broadcaster,
    config::{Config, QueueHubType},
    http_api::{start_http, State as HttpState},
    queue::{aof::AofQueueHub, in_memory::InMemoryQueueHub, QueueHub},
};
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

//TODO: graceful shutdown of QueueHub and Broadcaster
//TODO: limit allocated resources: batch size, message length, etc.
fn run_with<QH>(
    cfg: Config,
    queue_hub: QH,
) -> Pin<Box<dyn Future<Output = Result<()>>>>
where
    QH: QueueHub,
{
    let broadcaster = Broadcaster::new(
        cfg.listener_channel_size,
        cfg.listener_heartbeat_period,
    );
    let http_state = HttpState::new(queue_hub.clone(), broadcaster.clone());
    Box::pin(async move {
        for queue_name in queue_hub.queue_names().await? {
            broadcaster.create_channel(queue_name).await
        }
        task::spawn(garbage_collector(
            queue_hub,
            cfg.garbage_collection_period,
        ));
        start_http(http_state, cfg.http_sock_address)
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
        QueueHubType::AppendOnlyFile => {
            let queue_hub = AofQueueHub::load(
                cfg.data_directory.clone(),
                cfg.bytes_per_segment,
            )
            .await?;
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
