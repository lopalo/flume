pub mod in_memory;

use async_trait::async_trait;
use std::collections::HashMap;

type Payload = Vec<u8>;

#[async_trait]
pub trait QueueHub: Clone + Send + Sync + 'static {
    async fn create_queue(&self, queue_name: &str) -> CreateQueueResult;

    async fn delete_queue(&self, queue_name: &str) -> DeleteQueueResult;

    async fn reset(&self, queue_name: &str) -> ResetQueueResult;

    async fn push(
        &self,
        queue_name: &str,
        payload: Payload,
    ) -> PushMessageResult;

    async fn take(&self, queue_name: &str) -> TakeMessageResult;

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

pub enum PushMessageResult {
    Done,
    QueueDoesNotExist,
    QueueIsFull,
}

pub enum TakeMessageResult {
    Message { payload: Payload },
    QueueDoesNotExist,
    QueueIsEmpty,
}
