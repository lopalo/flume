use crate::queue::{Payload, QueueHub, QueueName};
use async_std::{
    channel::{self, Sender, TrySendError},
    sync::{Arc, RwLock},
    task,
};
use futures::{select, Future, FutureExt, StreamExt};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tide::sse::Sender as SseSender;

struct Message {
    id: String,
    payload: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct ListenerId(usize);

type Listeners = HashMap<ListenerId, Sender<Arc<Message>>>;

type Channels = HashMap<QueueName, RwLock<Listeners>>;

#[derive(Clone)]
pub struct Broadcaster {
    listener_channel_size: usize,
    heartbeat_period: Duration,
    next_listener_id: Arc<AtomicUsize>,
    channels: Arc<RwLock<Channels>>,
}

impl Broadcaster {
    pub fn new(
        listener_channel_size: usize,
        heartbeat_period: Duration,
    ) -> Self {
        Self {
            listener_channel_size,
            heartbeat_period,
            next_listener_id: Arc::new(AtomicUsize::new(0)),
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_channel(&self, queue_name: QueueName) {
        self.channels.write().await.entry(queue_name).or_default();
    }

    pub async fn delete_channel(&self, queue_name: &QueueName) {
        self.channels.write().await.remove(queue_name);
    }

    pub async fn listen(&self, sse: SseSender, queue_name: &QueueName) {
        let Self {
            listener_channel_size,
            heartbeat_period,
            next_listener_id,
            channels,
        } = self;
        let listener_id =
            ListenerId(next_listener_id.fetch_add(1, Ordering::Relaxed));
        let (sender, receiver) = channel::bounded(*listener_channel_size);
        if let Some(channel) = channels.read().await.get(queue_name) {
            channel.write().await.insert(listener_id, sender);
            log::debug!("Added SSE connection: {}", listener_id.0);
        } else {
            return;
        };

        let mut heartbeat_counter = 0;
        let mut receiver = receiver.fuse();
        loop {
            let res = select! {
                msg = receiver.next().fuse() => match msg {
                    Some(msg) =>
                        sse.send("message", &msg.payload, Some(&msg.id)).await,
                    None => break
                },
                _ = task::sleep(*heartbeat_period).fuse() => {
                    let count = heartbeat_counter.to_string();
                    heartbeat_counter += 1;
                    sse.send("heartbeat", count, None).await
                }

            };
            if let Err(_) = res {
                break;
            }
        }

        if let Some(channel) = channels.read().await.get(queue_name) {
            channel.write().await.remove(&listener_id);
            log::debug!("Removed SSE connection: {}", listener_id.0);
        };
    }

    pub async fn publish<'a, QH, It, Res, Fut>(
        &self,
        queue_name: &QueueName,
        fut: Fut,
    ) -> Result<Res, QH::Error>
    where
        QH: QueueHub,
        It: Iterator<Item = (QH::Position, &'a Payload<QH>)>,
        Fut: Future<Output = Result<(It, Res), QH::Error>>,
    {
        if let Some(channel) = self.channels.read().await.get(queue_name) {
            let mut channel = channel.write().await;
            //the future is run with an exclusive access to the channel
            let (for_publishing, res) = fut.await?;
            for (position, payload) in for_publishing {
                let msg = Arc::new(Message {
                    id: serde_json::to_string(&position).unwrap(),
                    payload: serde_json::to_string(payload).unwrap(),
                });
                let mut closed_listeners = vec![];
                for (listener_id, listener) in channel.iter() {
                    match listener.try_send(Arc::clone(&msg)) {
                        Ok(()) => (),
                        Err(TrySendError::Full(_)) => log::trace!(
                            "SSE connection '{}' is congested",
                            listener_id.0
                        ),
                        Err(TrySendError::Closed(_)) => {
                            closed_listeners.push(*listener_id)
                        }
                    };
                }
                for listener_id in closed_listeners.iter() {
                    channel.remove(listener_id);
                    log::debug!("Removed SSE connection: {}", listener_id.0);
                }
            }
            Ok(res)
        } else {
            Ok(fut.await?.1)
        }
    }
}
