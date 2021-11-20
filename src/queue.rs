pub mod in_memory;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use anyhow::Result;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

pub type Payloads = Vec<Payload>;
pub type Messages<Pos> = Vec<Message<Pos>>;

#[async_trait]
pub trait QueueHub: Clone + Send + Sync + 'static {
    type Position: Clone + Send + Sync + Serialize + DeserializeOwned;

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

#[derive(
    PartialEq, Eq, Hash, Default, Clone, Debug, Serialize, Deserialize,
)]
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

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct Consumer(String);

impl Consumer {
    pub fn new(consumer: String) -> Self {
        Self(consumer)
    }
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize,
)]
pub struct Payload(String);

impl Payload {
    pub fn new(payload: String) -> Self {
        Self(payload)
    }
}

#[derive(Clone, Serialize, Debug)]
pub struct Message<Pos>
where
    Pos: Serialize,
{
    pub position: Pos,
    pub payload: Payload,
}

#[derive(PartialEq, Eq, Debug)]
pub enum CreateQueueResult {
    Done,
    QueueAlreadyExists,
}

#[derive(PartialEq, Eq, Debug)]
pub enum DeleteQueueResult {
    Done,
    QueueDoesNotExist,
}

#[derive(PartialEq, Eq, Debug)]
pub enum AddConsumerResult {
    Done,
    QueueDoesNotExist,
    ConsumerAlreadyAdded,
}

#[derive(PartialEq, Eq, Debug)]
pub enum RemoveConsumerResult {
    Done,
    QueueDoesNotExist,
    UnknownConsumer,
}

#[derive(PartialEq, Eq, Debug)]
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

#[derive(PartialEq, Eq, Debug)]
pub enum CommitMessagesResult {
    Committed(usize),
    QueueDoesNotExist,
    UnknownConsumer,
    PositionIsOutOfQueue,
}

#[derive(PartialEq, Eq, Debug)]
pub enum GetConsumersResult {
    Consumers(Vec<Consumer>),
    QueueDoesNotExist,
}

#[derive(PartialEq, Eq, Debug, Serialize)]
pub struct QueueStats {
    pub size: usize,
    pub consumers: usize,
    pub min_unconsumed_size: usize,
    pub max_unconsumed_size: usize,
}

pub type Stats = HashMap<QueueName, QueueStats>;
