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
use async_std::{prelude::*, task};
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook_async_std::Signals;
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
        let gc_handle = task::spawn(garbage_collector(
            queue_hub.clone(),
            cfg.garbage_collection_period,
        ));
        let http_handle = task::spawn(async move {
            let res = start_http(http_state, cfg.http_sock_address).await;
            if let Err(err) = res {
                log::error!("Couldn't start HTTP server: {}", err);
            }
        });
        let mut signals = Signals::new(&[SIGINT, SIGTERM])?;
        if let Some(_) = signals.next().await {
            log::info!("Shutting down services");
            http_handle.cancel().await;
            gc_handle.cancel().await;
            queue_hub.collect_garbage().await?;
            log::info!("All services have shut down");
        }
        Ok(())
    })
}

pub async fn setup_server() -> Result<()> {
    let cfg = config::read_config();

    femme::with_level(cfg.log_level);

    let res = async {
        match cfg.queue_hub_type {
            QueueHubType::InMemory => {
                let queue_hub = InMemoryQueueHub::new(cfg.max_queue_size);
                run_with(cfg, queue_hub)
            }
            QueueHubType::AppendOnlyFile => {
                let queue_hub = AofQueueHub::load(
                    cfg.data_directory.clone(),
                    cfg.bytes_per_segment,
                    cfg.opened_files_per_segment,
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
        .await
    }
    .await;
    if let Err(err) = res {
        log::error!("Error during initialization: {}", err);
    }
    Ok(())
}
