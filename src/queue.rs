use async_std::sync::{Arc, Mutex, RwLock};
use patricia_tree::PatriciaMap;
use std::collections::{HashMap, VecDeque};

type Message = Vec<u8>;

type Queues = RwLock<PatriciaMap<Mutex<VecDeque<Message>>>>;

#[derive(Clone)]
pub struct QueueHub {
    max_queue_size: usize,
    queues: Arc<Queues>,
}

impl QueueHub {
    pub fn new(max_queue_size: usize) -> Self {
        Self {
            max_queue_size,
            queues: Arc::new(RwLock::new(PatriciaMap::new())),
        }
    }

    pub async fn create_queue(&self, queue_name: &str) -> CreateQueueResult {
        use CreateQueueResult::*;

        let mut qs = self.queues.write().await;
        if qs.contains_key(queue_name) {
            AlreadyExists
        } else {
            qs.insert(queue_name, Mutex::new(VecDeque::new()));
            Done
        }
    }

    pub async fn delete_queue(&self, queue_name: &str) -> DeleteQueueResult {
        use DeleteQueueResult::*;

        let mut qs = self.queues.write().await;
        if qs.contains_key(queue_name) {
            qs.remove(queue_name);
            Done
        } else {
            DoesNotExist
        }
    }

    pub async fn reset(&self, queue_name: &str) -> ResetQueueResult {
        use ResetQueueResult::*;
        if let Some(q) = self.queues.read().await.get(queue_name) {
            q.lock().await.clear();
            Done
        } else {
            DoesNotExist
        }
    }

    pub async fn push(
        &self,
        queue_name: &str,
        message: Message,
    ) -> PushMessageResult {
        use PushMessageResult::*;

        let qs = self.queues.read().await;
        match qs.get(queue_name) {
            Some(q) => {
                let mut q = q.lock().await;
                if q.len() < self.max_queue_size {
                    q.push_back(message);
                    Done
                } else {
                    QueueIsFull
                }
            }
            None => QueueDoesNotExist,
        }
    }

    pub async fn take(&self, queue_name: &str) -> TakeMessageResult {
        use TakeMessageResult::*;

        let qs = self.queues.read().await;
        match qs.get(queue_name) {
            Some(q) => match q.lock().await.pop_front() {
                Some(payload) => Message { payload },
                None => QueueIsEmpty,
            },
            None => QueueDoesNotExist,
        }
    }

    pub async fn size(
        &self,
        queue_name_prefix: &str,
    ) -> HashMap<String, usize> {
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

pub enum CreateQueueResult {
    Done,
    AlreadyExists,
}

pub enum DeleteQueueResult {
    Done,
    DoesNotExist,
}

pub enum ResetQueueResult {
    Done,
    DoesNotExist,
}

pub enum PushMessageResult {
    Done,
    QueueDoesNotExist,
    QueueIsFull,
}

pub enum TakeMessageResult {
    Message { payload: Message },
    QueueDoesNotExist,
    QueueIsEmpty,
}
