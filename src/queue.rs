pub mod in_memory;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, error::Error as StdError, fmt::Debug};

pub type Messages<Pos, Data> = Vec<Message<Pos, Data>>;

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

    fn payload(data: String) -> Payload<Self::PayloadData>;

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
        batch: &[Payload<Self::PayloadData>],
    ) -> Result<PushMessagesResult, Self::Error>;

    async fn read(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<
        ReadMessagesResult<Self::Position, Self::PayloadData>,
        Self::Error,
    >;

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
    ) -> Result<
        ReadMessagesResult<Self::Position, Self::PayloadData>,
        Self::Error,
    >;

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

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Payload<Data>(Data);

impl<Data> Payload<Data> {
    pub fn new(data: Data) -> Self {
        Self(data)
    }
}

#[derive(Clone, Serialize, Debug)]
pub struct Message<Pos, Data> {
    pub position: Pos,
    pub payload: Payload<Data>,
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

pub enum ReadMessagesResult<Pos, Data> {
    Messages(Messages<Pos, Data>),
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
