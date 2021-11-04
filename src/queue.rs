pub mod in_memory;

pub use self::queue_name::QueueName;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type Payloads = Vec<Payload>;
pub type Messages<Pos> = Vec<Message<Pos>>;

#[async_trait]
pub trait QueueHub: Clone + Send + Sync + 'static {
    type Position: Serialize;

    async fn create_queue(&self, queue_name: QueueName) -> CreateQueueResult;

    async fn delete_queue(&self, queue_name: &QueueName) -> DeleteQueueResult;

    async fn add_consumer(
        &self,
        queue_name: &QueueName,
        consumer: Consumer,
    ) -> AddConsumerResult;

    async fn remove_consumer(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
    ) -> RemoveConsumerResult;

    async fn push(
        &self,
        queue_name: &QueueName,
        batch: Payloads,
    ) -> PushMessagesResult;

    async fn read(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> ReadMessagesResult<Self::Position>;

    async fn commit(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        position: &Self::Position,
    ) -> CommitMessagesResult;

    async fn take(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> ReadMessagesResult<Self::Position>;

    //TODO: collect_garbage method

    //TODO: size of queues for each consumer for each queue
    async fn size(
        &self,
        queue_name_prefix: &QueueName,
    ) -> HashMap<QueueName, usize>;
}

mod queue_name {
    use serde::{Deserialize, Serialize};

    #[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
    pub struct QueueName(String);

    impl QueueName {
        pub fn new(name: String) -> Self {
            QueueName(name)
        }
    }

    impl AsRef<[u8]> for QueueName {
        fn as_ref(&self) -> &[u8] {
            self.0.as_bytes()
        }
    }
}

#[derive(PartialEq, Eq, Hash, Deserialize)]
pub struct Consumer(String);

#[derive(Clone, Serialize, Deserialize)]
pub struct Payload(String);

#[derive(Clone, Serialize)]
pub struct Message<Pos>
where
    Pos: Serialize,
{
    position: Pos,
    payload: Payload,
}

pub enum CreateQueueResult {
    Done,
    QueueAlreadyExists,
}

pub enum DeleteQueueResult {
    Done,
    QueueDoesNotExist,
}

pub enum AddConsumerResult {
    Done,
    QueueDoesNotExist,
    ConsumerAlreadyAdded,
}

pub enum RemoveConsumerResult {
    Done,
    QueueDoesNotExist,
    UnknownConsumer,
}

pub enum PushMessagesResult {
    Done,
    NoSpaceInQueue,
    QueueDoesNotExist,
}

pub enum ReadMessagesResult<Pos>
where
    Pos: Serialize,
{
    Messages(Messages<Pos>),
    QueueDoesNotExist,
    UnknownConsumer,
}

pub enum CommitMessagesResult {
    // Committed ( usize ),
// QueueDoesNotExist,
}
