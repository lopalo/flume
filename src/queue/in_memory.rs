use async_std::sync::{Arc, Mutex, RwLock};
use async_trait::async_trait;
use patricia_tree::PatriciaMap;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::Entry, HashMap, VecDeque};

use super::*;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Pos(usize);

impl Pos {
    fn incr(&mut self) {
        self.0 += 1;
    }

    fn incr_by(&mut self, n: usize) {
        self.0 += n;
    }
}

struct Queue {
    next_position: Mutex<Pos>,
    messages: RwLock<VecDeque<Message<Pos>>>,
    consumers: RwLock<HashMap<Consumer, Mutex<Pos>>>,
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
}

#[async_trait]
impl QueueHub for InMemoryQueueHub {
    type Position = Pos;

    async fn create_queue(&self, queue_name: QueueName) -> CreateQueueResult {
        use CreateQueueResult::*;

        let mut qs = self.queues.write().await;
        if qs.contains_key(&queue_name) {
            QueueAlreadyExists
        } else {
            let q = Queue {
                next_position: Mutex::new(Pos(0)),
                messages: RwLock::new(VecDeque::new()),
                consumers: RwLock::new(HashMap::new()),
            };
            qs.insert(queue_name, q);
            Done
        }
    }

    async fn delete_queue(&self, queue_name: &QueueName) -> DeleteQueueResult {
        use DeleteQueueResult::*;

        let mut qs = self.queues.write().await;
        if qs.contains_key(&queue_name) {
            qs.remove(queue_name);
            Done
        } else {
            QueueDoesNotExist
        }
    }

    async fn add_consumer(
        &self,
        queue_name: &QueueName,
        consumer: Consumer,
    ) -> AddConsumerResult {
        use AddConsumerResult::*;

        if let Some(q) = self.queues.read().await.get(queue_name) {
            let next_pos = q.next_position.lock().await;
            let position = q
                .messages
                .read()
                .await
                .front()
                .map(|m| m.position.clone())
                .unwrap_or_else(|| next_pos.clone());
            match q.consumers.write().await.entry(consumer) {
                Entry::Occupied(_) => ConsumerAlreadyAdded,
                Entry::Vacant(entry) => {
                    entry.insert(Mutex::new(position));
                    Done
                }
            }
        } else {
            QueueDoesNotExist
        }
    }

    async fn remove_consumer(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
    ) -> RemoveConsumerResult {
        use RemoveConsumerResult::*;

        if let Some(q) = self.queues.read().await.get(queue_name) {
            match q.consumers.write().await.remove(consumer) {
                Some(_) => Done,
                None => UnknownConsumer,
            }
        } else {
            QueueDoesNotExist
        }
    }

    async fn push(
        &self,
        queue_name: &QueueName,
        batch: Payloads,
    ) -> PushMessagesResult {
        use PushMessagesResult::*;

        let qs = self.queues.read().await;
        match qs.get(queue_name) {
            Some(q) => {
                let mut next_position = q.next_position.lock().await;
                let mut messages = q.messages.write().await;
                if messages.len() + batch.len() <= self.max_queue_size {
                    messages.extend(batch.into_iter().map(|payload| {
                        let position = next_position.clone();
                        next_position.incr();
                        Message { position, payload }
                    }));
                    Done
                } else {
                    NoSpaceInQueue
                }
            }
            None => QueueDoesNotExist,
        }
    }

    async fn read(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> ReadMessagesResult<Self::Position> {
        use ReadMessagesResult::*;

        let qs = self.queues.read().await;
        match qs.get(queue_name) {
            Some(q) => {
                let messages = q.messages.read().await;
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(pos_mutex) => {
                        let pos_guard = pos_mutex.lock().await;
                        let pos = &*pos_guard;
                        let start_idx = messages
                            .binary_search_by_key(pos, |m| m.position.clone());
                        let start_idx = match start_idx {
                            Ok(idx) => idx,
                            Err(_) => return Messages(vec![]),
                        };
                        let end_idx = (start_idx + number).min(messages.len());
                        let batch: Vec<_> = messages
                            .range(start_idx..end_idx)
                            .map(|msg| msg.clone())
                            .collect();
                        Messages(batch)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        }
    }

    async fn commit(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        position: &Self::Position,
    ) -> CommitMessagesResult {
        use CommitMessagesResult::*;

        let qs = self.queues.read().await;
        match qs.get(queue_name) {
            Some(q) => {
                let messages = q.messages.read().await;
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(pos_mutex) => {
                        let mut pos_guard = pos_mutex.lock().await;
                        let consumer_pos = &mut *pos_guard;
                        if position < consumer_pos {
                            return PositionIsOutOfQueue;
                        }
                        let idx = messages
                            .binary_search_by_key(position, |m| {
                                m.position.clone()
                            });
                        if let Err(..) = idx {
                            return PositionIsOutOfQueue;
                        }
                        let prev_pos = consumer_pos.clone();
                        *consumer_pos = position.clone();
                        consumer_pos.incr();
                        let committed = consumer_pos.0 - prev_pos.0;
                        Committed(committed)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        }
    }

    async fn take(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> ReadMessagesResult<Self::Position> {
        use ReadMessagesResult::*;

        let qs = self.queues.read().await;
        match qs.get(queue_name) {
            Some(q) => {
                let messages = q.messages.read().await;
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(pos_mutex) => {
                        let mut pos_guard = pos_mutex.lock().await;
                        let pos = &mut *pos_guard;
                        let start_idx = messages
                            .binary_search_by_key(pos, |m| m.position.clone());
                        let start_idx = match start_idx {
                            Ok(idx) => idx,
                            Err(_) => return Messages(vec![]),
                        };
                        let end_idx = (start_idx + number).min(messages.len());
                        let batch: Vec<_> = messages
                            .range(start_idx..end_idx)
                            .map(|msg| msg.clone())
                            .collect();
                        pos.incr_by(batch.len());
                        Messages(batch)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        }
    }

    async fn stats(&self, queue_name_prefix: &QueueName) -> Stats {
        let mut res = HashMap::new();
        let qs = self.queues.read().await;
        let q_names = qs.iter_prefix(queue_name_prefix.as_ref());
        for (queue_name, queue) in q_names {
            let last_pos = queue.next_position.lock().await.0;
            let messages = queue.messages.read().await;
            let size = messages.len();
            let mut min_consumer_pos = last_pos;
            let mut max_consumer_pos =
                messages.front().map(|m| m.position.0).unwrap_or(last_pos);
            drop(messages);
            let consumers = queue.consumers.read().await;
            for consumer_pos in consumers.values() {
                let consumer_pos = consumer_pos.lock().await.0;
                min_consumer_pos = min_consumer_pos.min(consumer_pos);
                max_consumer_pos = max_consumer_pos.max(consumer_pos);
            }
            let q_stats = QueueStats {
                size,
                consumers: consumers.len(),
                min_unconsumed_size: last_pos - max_consumer_pos,
                max_unconsumer_size: last_pos - min_consumer_pos,
            };
            res.insert(
                QueueName::new(String::from_utf8(queue_name).unwrap()),
                q_stats,
            );
        }
        res
    }
}
