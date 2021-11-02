use async_std::sync::{Arc, Mutex, RwLock};
use async_trait::async_trait;
use patricia_tree::PatriciaMap;
use std::collections::{HashMap, VecDeque};

use super::{
    Batch, CreateQueueResult, DeleteQueueResult, Payload, PushMessagesResult,
    QueueHub, ResetQueueResult, TakeMessagesResult,
};

type Queues = RwLock<PatriciaMap<Mutex<VecDeque<Payload>>>>;

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
    type Position = usize;

    async fn create_queue(&self, queue_name: &str) -> CreateQueueResult {
        use CreateQueueResult::*;

        let mut qs = self.queues.write().await;
        if qs.contains_key(queue_name) {
            AlreadyExists
        } else {
            qs.insert(queue_name, Mutex::new(VecDeque::new()));
            Done
        }
    }

    async fn delete_queue(&self, queue_name: &str) -> DeleteQueueResult {
        use DeleteQueueResult::*;

        let mut qs = self.queues.write().await;
        if qs.contains_key(queue_name) {
            qs.remove(queue_name);
            Done
        } else {
            DoesNotExist
        }
    }

    async fn reset(&self, queue_name: &str) -> ResetQueueResult {
        use ResetQueueResult::*;
        if let Some(q) = self.queues.read().await.get(queue_name) {
            q.lock().await.clear();
            Done
        } else {
            DoesNotExist
        }
    }

    async fn push(&self, queue_name: &str, batch: Batch) -> PushMessagesResult {
        use PushMessagesResult::*;

        let qs = self.queues.read().await;
        match qs.get(queue_name) {
            Some(q) => {
                let mut q = q.lock().await;
                if q.len() + batch.len() <= self.max_queue_size {
                    q.extend(batch);
                    Done
                } else {
                    NoSpaceInQueue
                }
            }
            None => QueueDoesNotExist,
        }
    }

    async fn take(
        &self,
        queue_name: &str,
        mut number: usize,
    ) -> TakeMessagesResult {
        use TakeMessagesResult::*;

        let qs = self.queues.read().await;
        match qs.get(queue_name) {
            Some(q) => {
                let mut q = q.lock().await;
                number = number.min(q.len());
                let batch = q.drain(..number).collect();
                Messages { batch }
            }
            None => QueueDoesNotExist,
        }
    }

    async fn size(&self, queue_name_prefix: &str) -> HashMap<String, usize> {
        let mut res = HashMap::new();
        let qs = self.queues.read().await;
        let q_names = qs.iter_prefix(queue_name_prefix.as_bytes());
        for (queue_name, queue) in q_names {
            res.insert(
                String::from_utf8(queue_name).unwrap(),
                queue.lock().await.len(),
            );
        }
        res
    }
}
