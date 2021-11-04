use async_std::sync::{Arc, Mutex, RwLock};
use async_trait::async_trait;
use patricia_tree::PatriciaMap;
use serde::Serialize;
use std::collections::{hash_map::Entry, HashMap, VecDeque};

use super::*;

#[derive(Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
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
    next_position: Pos,
    consumers: HashMap<Consumer, Pos>,
    messages: VecDeque<Message<Pos>>,
}

type Queues = RwLock<PatriciaMap<Mutex<Queue>>>;

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
                next_position: Pos(0),
                consumers: HashMap::new(),
                messages: VecDeque::new(),
            };
            qs.insert(queue_name, Mutex::new(q));
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
            let mut q = q.lock().await;
            let position = q
                .messages
                .front()
                .map(|m| m.position.clone())
                .unwrap_or_else(|| q.next_position.clone());
            match q.consumers.entry(consumer) {
                Entry::Occupied(_) => ConsumerAlreadyAdded,
                Entry::Vacant(entry) => {
                    entry.insert(position);
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
            match q.lock().await.consumers.remove(consumer) {
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
            Some(q_mutex) => {
                let mut q_guard = q_mutex.lock().await;
                // Extracts Queue from MutexGuard to split
                // the mutable borrow for multiple fields
                let q: &mut Queue = &mut q_guard;
                if q.messages.len() + batch.len() <= self.max_queue_size {
                    let next_position = &mut q.next_position;
                    q.messages.extend(batch.into_iter().map(|payload| {
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
        _queue_name: &QueueName,
        _consumer: &Consumer,
        _number: usize,
    ) -> ReadMessagesResult<Self::Position> {
        //TODO:
        unimplemented!()
    }

    async fn commit(
        &self,
        _queue_name: &QueueName,
        _consumer: &Consumer,
        _position: &Self::Position,
    ) -> CommitMessagesResult {
        //TODO:
        unimplemented!()
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
            Some(q_mutex) => {
                let mut q_guard = q_mutex.lock().await;
                let q: &mut Queue = &mut q_guard;
                let messages = &mut q.messages;
                match q.consumers.get_mut(consumer) {
                    Some(pos) => {
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

    async fn size(
        &self,
        queue_name_prefix: &QueueName,
    ) -> HashMap<QueueName, usize> {
        let mut res = HashMap::new();
        let qs = self.queues.read().await;
        let q_names = qs.iter_prefix(queue_name_prefix.as_ref());
        for (queue_name, queue) in q_names {
            res.insert(
                QueueName::new(String::from_utf8(queue_name).unwrap()),
                queue.lock().await.messages.len(),
            );
        }
        res
    }
}
