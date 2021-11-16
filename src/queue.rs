pub mod in_memory;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub use self::queue_name::QueueName;
use anyhow::Result;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

pub type Payloads = Vec<Payload>;
pub type Messages<Pos> = Vec<Message<Pos>>;

#[async_trait]
pub trait QueueHub: Clone + Send + Sync + 'static {
    type Position: Send + Sync + Serialize + DeserializeOwned;

    async fn create_queue(
        &self,
        queue_name: QueueName,
    ) -> Result<CreateQueueResult>;

    async fn delete_queue(
        &self,
        queue_name: &QueueName,
    ) -> Result<DeleteQueueResult>;

    async fn add_consumer(
        &self,
        queue_name: &QueueName,
        consumer: Consumer,
    ) -> Result<AddConsumerResult>;

    async fn remove_consumer(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
    ) -> Result<RemoveConsumerResult>;

    async fn push(
        &self,
        queue_name: &QueueName,
        batch: Payloads,
    ) -> Result<PushMessagesResult>;

    async fn read(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self::Position>>;

    async fn commit(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        position: &Self::Position,
    ) -> Result<CommitMessagesResult>;

    async fn take(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self::Position>>;

    async fn collect_garbage(&self) -> Result<()>;

    async fn queue_names(&self) -> Result<Vec<QueueName>>;

    async fn consumers(
        &self,
        queue_name: &QueueName,
    ) -> Result<GetConsumersResult>;

    async fn stats(&self, queue_name_prefix: &QueueName) -> Result<Stats>;
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

    impl AsRef<str> for QueueName {
        fn as_ref(&self) -> &str {
            &self.0
        }
    }

    impl AsRef<[u8]> for QueueName {
        fn as_ref(&self) -> &[u8] {
            self.0.as_bytes()
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
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
    Committed(usize),
    QueueDoesNotExist,
    UnknownConsumer,
    PositionIsOutOfQueue,
}

pub enum GetConsumersResult {
    Consumers(Vec<Consumer>),
    QueueDoesNotExist,
}

#[derive(Serialize)]
pub struct QueueStats {
    size: usize,
    consumers: usize,
    min_unconsumed_size: usize,
    max_unconsumed_size: usize,
}

pub type Stats = HashMap<QueueName, QueueStats>;
