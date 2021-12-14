pub mod aof;
pub mod in_memory;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use async_trait::async_trait;
use derivative::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, error::Error as StdError, fmt::Debug};

pub type Messages<QH> = Vec<Message<QH>>;

#[async_trait]
pub trait QueueHub: Clone + Send + Sync + 'static {
    type Position: Clone + Send + Sync + Serialize + DeserializeOwned;
    type PayloadData: Clone
        + Send
        + Sync
        + PartialEq
        + Eq
        + Debug
        + Serialize
        + DeserializeOwned;
    type Error: StdError + Send + Sync + 'static;

    fn payload(data: String) -> Payload<Self>;

    async fn create_queue(
        &self,
        queue_name: QueueName,
    ) -> Result<CreateQueueResult, Self::Error>;

    async fn delete_queue(
        &self,
        queue_name: &QueueName,
    ) -> Result<DeleteQueueResult, Self::Error>;

    async fn add_consumer(
        &self,
        queue_name: &QueueName,
        consumer: Consumer,
    ) -> Result<AddConsumerResult, Self::Error>;

    async fn remove_consumer(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
    ) -> Result<RemoveConsumerResult, Self::Error>;

    async fn push(
        &self,
        queue_name: &QueueName,
        batch: &[Payload<Self>],
    ) -> Result<PushMessagesResult<Self>, Self::Error>;

    async fn read(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self>, Self::Error>;

    async fn commit(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        position: &Self::Position,
    ) -> Result<CommitMessagesResult, Self::Error>;

    async fn take(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self>, Self::Error>;

    async fn collect_garbage(&self) -> Result<(), Self::Error>;

    async fn queue_names(&self) -> Result<Vec<QueueName>, Self::Error>;

    async fn consumers(
        &self,
        queue_name: &QueueName,
    ) -> Result<GetConsumersResult, Self::Error>;

    async fn stats(
        &self,
        queue_name_prefix: &QueueName,
    ) -> Result<Stats, Self::Error>;
}

fn is_valid_name(name: &str) -> bool {
    for c in name.chars() {
        if c == '_' || c.is_ascii_alphanumeric() {
            continue;
        }
        return false;
    }
    true
}

#[derive(
    PartialEq, Eq, Hash, Default, Clone, Debug, Serialize, Deserialize,
)]
pub struct QueueName(String);

impl QueueName {
    pub fn new(name: String) -> Self {
        QueueName(name)
    }

    pub fn is_valid(&self) -> bool {
        return is_valid_name(&self.0);
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

    pub fn is_valid(&self) -> bool {
        return is_valid_name(&self.0);
    }
}

#[derive(Clone, Serialize, Deserialize, Derivative)]
#[derivative(Debug(bound = ""), PartialEq(bound = ""), Eq(bound = ""))]
pub struct Payload<QH: QueueHub>(QH::PayloadData);

impl<QH: QueueHub> Payload<QH> {
    pub fn new(data: QH::PayloadData) -> Self {
        Self(data)
    }
}

#[derive(Clone, Serialize)]
#[serde(bound = "")]
pub struct Message<QH: QueueHub> {
    pub position: QH::Position,
    pub payload: Payload<QH>,
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
pub enum PushMessagesResult<QH: QueueHub> {
    Done(Vec<QH::Position>),
    NoSpaceInQueue,
    QueueDoesNotExist,
}

pub enum ReadMessagesResult<QH: QueueHub> {
    Messages(Messages<QH>),
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
