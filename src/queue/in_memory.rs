use async_std::sync::{Arc, RwLock};
use async_trait::async_trait;
use patricia_tree::PatriciaMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    fmt::{self, Display},
    result::Result as StdResult,
};

use super::*;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Pos(usize);

#[derive(Debug)]
pub struct Error;

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "in-memory queue hub error")
    }
}

impl StdError for Error {}

type Result<T> = StdResult<T, Error>;

struct MessageStore {
    next_position: Pos,
    messages: VecDeque<Message<InMemoryQueueHub>>,
}

struct Queue {
    consumers: RwLock<HashMap<Consumer, RwLock<Pos>>>,
    store: RwLock<MessageStore>,
}

type Queues = RwLock<PatriciaMap<Queue>>;

#[derive(Clone)]
pub struct InMemoryQueueHub {
    max_queue_size: usize,
    queues: Arc<Queues>,
}

impl InMemoryQueueHub {
    pub fn new(max_queue_size: usize) -> Self {
        Self {
            max_queue_size,
            queues: Arc::new(RwLock::new(PatriciaMap::new())),
        }
    }

    async fn read_messages(
        messages: &VecDeque<Message<Self>>,
        start_pos: &Pos,
        number: usize,
    ) -> Messages<Self> {
        let start_idx =
            messages.binary_search_by_key(start_pos, |m| m.position.clone());
        let start_idx = match start_idx {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        let end_idx = (start_idx + number).min(messages.len());
        messages
            .range(start_idx..end_idx)
            .map(|msg| msg.clone())
            .collect()
    }
}

#[async_trait]
impl QueueHub for InMemoryQueueHub {
    type Position = Pos;
    type PayloadData = Arc<String>;
    type Error = Error;

    fn payload(data: String) -> Payload<Self> {
        Payload::new(Arc::new(data))
    }

    async fn create_queue(
        &self,
        queue_name: QueueName,
    ) -> Result<CreateQueueResult> {
        use CreateQueueResult::*;

        let mut qs = self.queues.write().await;
        Ok(if qs.contains_key(&queue_name) {
            QueueAlreadyExists
        } else {
            let store = MessageStore {
                next_position: Pos(0),
                messages: VecDeque::new(),
            };
            let q = Queue {
                consumers: RwLock::new(HashMap::new()),
                store: RwLock::new(store),
            };
            qs.insert(queue_name, q);
            Done
        })
    }

    async fn delete_queue(
        &self,
        queue_name: &QueueName,
    ) -> Result<DeleteQueueResult> {
        use DeleteQueueResult::*;

        let mut qs = self.queues.write().await;
        Ok(if qs.contains_key(&queue_name) {
            qs.remove(queue_name);
            Done
        } else {
            QueueDoesNotExist
        })
    }

    async fn add_consumer(
        &self,
        queue_name: &QueueName,
        consumer: Consumer,
    ) -> Result<AddConsumerResult> {
        use AddConsumerResult::*;

        Ok(if let Some(q) = self.queues.read().await.get(queue_name) {
            match q.consumers.write().await.entry(consumer) {
                Entry::Occupied(_) => ConsumerAlreadyAdded,
                Entry::Vacant(entry) => {
                    entry.insert(RwLock::new(Pos(0)));
                    Done
                }
            }
        } else {
            QueueDoesNotExist
        })
    }

    async fn remove_consumer(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
    ) -> Result<RemoveConsumerResult> {
        use RemoveConsumerResult::*;

        Ok(if let Some(q) = self.queues.read().await.get(queue_name) {
            match q.consumers.write().await.remove(consumer) {
                Some(_) => Done,
                None => UnknownConsumer,
            }
        } else {
            QueueDoesNotExist
        })
    }

    async fn push(
        &self,
        queue_name: &QueueName,
        batch: &[Payload<Self>],
    ) -> Result<PushMessagesResult<Self>> {
        use PushMessagesResult::*;

        let qs = self.queues.read().await;
        Ok(match qs.get(queue_name) {
            Some(q) => {
                let mut store_guard = q.store.write().await;
                //get a reference to inner struct to split the borrow
                let store = &mut *store_guard;
                let next_position = &mut store.next_position;
                let messages = &mut store.messages;
                if messages.len() + batch.len() <= self.max_queue_size {
                    let mut positions = Vec::with_capacity(batch.len());
                    messages.extend(batch.into_iter().map(|payload| {
                        let position = next_position.clone();
                        next_position.0 += 1;
                        positions.push(position.clone());
                        Message {
                            position,
                            payload: payload.clone(),
                        }
                    }));
                    Done(positions)
                } else {
                    NoSpaceInQueue
                }
            }
            None => QueueDoesNotExist,
        })
    }

    async fn read(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self>> {
        use ReadMessagesResult::*;

        let qs = self.queues.read().await;
        Ok(match qs.get(queue_name) {
            Some(q) => {
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(pos_lock) => {
                        let start_pos = pos_lock.read().await;
                        let messages = &q.store.read().await.messages;
                        let batch =
                            Self::read_messages(&messages, &start_pos, number)
                                .await;
                        Messages(batch)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        })
    }

    async fn commit(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        position: &Self::Position,
    ) -> Result<CommitMessagesResult> {
        use CommitMessagesResult::*;

        let qs = self.queues.read().await;
        Ok(match qs.get(queue_name) {
            Some(q) => {
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(pos_lock) => {
                        let mut consumer_pos = pos_lock.write().await;
                        if *position < *consumer_pos {
                            return Ok(PositionIsOutOfQueue);
                        }
                        let messages = &q.store.read().await.messages;
                        let idx = messages
                            .binary_search_by_key(position, |m| {
                                m.position.clone()
                            });
                        if let Err(..) = idx {
                            return Ok(PositionIsOutOfQueue);
                        }
                        let mut prev_pos = consumer_pos.clone();
                        if let Some(first_msg) = messages.front() {
                            if prev_pos < first_msg.position {
                                prev_pos = first_msg.position.clone();
                            }
                        }
                        *consumer_pos = position.clone();
                        consumer_pos.0 += 1;
                        let committed = consumer_pos.0 - prev_pos.0;
                        Committed(committed)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        })
    }

    async fn take(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self>> {
        use ReadMessagesResult::*;

        let qs = self.queues.read().await;
        Ok(match qs.get(queue_name) {
            Some(q) => {
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(pos_lock) => {
                        let mut pos = pos_lock.write().await;
                        let messages = &q.store.read().await.messages;
                        let batch =
                            Self::read_messages(&messages, &pos, number).await;
                        if let Some(last_msg) = batch.last() {
                            *pos = last_msg.position.clone();
                            pos.0 += 1;
                        }
                        Messages(batch)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        })
    }

    async fn collect_garbage(&self) -> Result<()> {
        for queue in self.queues.read().await.values() {
            let consumers = queue.consumers.read().await;
            if consumers.is_empty() {
                continue;
            };
            let mut min_pos = Pos(usize::MAX);
            for consumer_pos in consumers.values() {
                min_pos = min_pos.min(consumer_pos.read().await.clone());
            }
            let messages = &mut queue.store.write().await.messages;
            let start_idx =
                messages.binary_search_by_key(&min_pos, |m| m.position.clone());
            let idx = match start_idx {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            messages.drain(..idx);
        }
        Ok(())
    }

    async fn queue_names(&self) -> Result<Vec<QueueName>> {
        Ok(self
            .queues
            .read()
            .await
            .keys()
            .map(|name| QueueName::new(String::from_utf8(name).unwrap()))
            .collect())
    }

    async fn consumers(
        &self,
        queue_name: &QueueName,
    ) -> Result<GetConsumersResult> {
        use GetConsumersResult::*;

        Ok(if let Some(q) = self.queues.read().await.get(queue_name) {
            Consumers(
                q.consumers.read().await.keys().map(|c| c.clone()).collect(),
            )
        } else {
            QueueDoesNotExist
        })
    }

    async fn stats(&self, queue_name_prefix: &QueueName) -> Result<Stats> {
        let mut res = HashMap::new();
        let qs = self.queues.read().await;
        let q_names = qs.iter_prefix(queue_name_prefix.as_ref());
        for (queue_name, queue) in q_names {
            let consumers = queue.consumers.read().await;
            let mut consumer_positions = vec![];
            for consumer_pos in consumers.values() {
                consumer_positions.push(consumer_pos.read().await.0);
            }
            let store = queue.store.read().await;
            let last_pos = store.next_position.0;
            let first_pos = store
                .messages
                .front()
                .map(|m| m.position.0)
                .unwrap_or(last_pos);
            let size = store.messages.len();
            let mut min_consumer_pos = last_pos;
            let mut max_consumer_pos = first_pos;
            for mut consumer_pos in consumer_positions {
                consumer_pos = consumer_pos.max(first_pos);
                min_consumer_pos = min_consumer_pos.min(consumer_pos);
                max_consumer_pos = max_consumer_pos.max(consumer_pos);
            }
            let q_stats = QueueStats {
                size,
                consumers: consumers.len(),
                min_unconsumed_size: last_pos - max_consumer_pos,
                max_unconsumed_size: last_pos - min_consumer_pos,
            };
            res.insert(
                QueueName::new(String::from_utf8(queue_name).unwrap()),
                q_stats,
            );
        }
        Ok(res)
    }
}
