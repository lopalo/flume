pub mod in_memory;

use async_trait::async_trait;
use std::collections::HashMap;

type Payload = Vec<u8>;

type Batch = Vec<Payload>;

#[async_trait]
pub trait QueueHub: Clone + Send + Sync + 'static {
    type Position;

    async fn create_queue(&self, queue_name: &str) -> CreateQueueResult;

    async fn delete_queue(&self, queue_name: &str) -> DeleteQueueResult;

    async fn reset(&self, queue_name: &str) -> ResetQueueResult;

    async fn push(&self, queue_name: &str, batch: Batch) -> PushMessagesResult;

    async fn take(&self, queue_name: &str, number: usize)
        -> TakeMessagesResult;

    async fn size(&self, queue_name_prefix: &str) -> HashMap<String, usize>;
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

pub enum PushMessagesResult {
    Done,
    QueueDoesNotExist,
    NoSpaceInQueue,
}

pub enum TakeMessagesResult {
    Messages { batch: Batch },
    QueueDoesNotExist,
}
