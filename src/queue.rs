use async_std::sync::{Arc, Mutex};
use std::collections::VecDeque;

type Message = Vec<u8>;

#[derive(Clone)]
pub struct Queue {
    q: Arc<Mutex<VecDeque<Message>>>,
}

impl Queue {
    pub fn new() -> Self {
        Self {
            q: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub async fn push(&self, message: Message) {
        self.q.lock().await.push_back(message)
    }

    pub async fn take(&self) -> Option<Message> {
        self.q.lock().await.pop_front()
    }

    pub async fn reset(&self) {
        self.q.lock().await.clear()
    }

    pub async fn size(&self) -> usize {
        self.q.lock().await.len()
    }
}
