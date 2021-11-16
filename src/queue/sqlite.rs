use anyhow::Result;
use sqlx::{
    error::DatabaseError,
    migrate::Migrator,
    sqlite::{
        Sqlite, SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode,
        SqlitePool, SqliteSynchronous,
    },
    ConnectOptions, Error, FromRow, Transaction,
};
use std::{borrow::Borrow, path::Path};
use tide::log::LevelFilter;

use super::*;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MsgPk(i64);

#[derive(Clone)]
pub struct SqliteQueueHub {
    pool: SqlitePool,
}

struct ConsumerMsgId {
    consumer_id: i64,
    committed_msg_id: i64,
}

#[derive(FromRow)]
struct QueueMsgId {
    queue_id: i64,
    min_committed_msg_id: i64,
}

#[derive(FromRow)]
struct StatsRecord {
    queue_name: String,
    size: Option<i64>,
    consumers: Option<i64>,
    min_msg_count: Option<i64>,
    max_msg_count: Option<i64>,
}

impl SqliteQueueHub {
    pub async fn connect(mut options: SqliteConnectOptions) -> Result<Self> {
        options = options
            .create_if_missing(true)
            .synchronous(SqliteSynchronous::Normal)
            .locking_mode(SqliteLockingMode::Exclusive)
            .journal_mode(SqliteJournalMode::Truncate);
        options.log_statements(LevelFilter::Debug);

        let pool = SqlitePool::connect_with(options).await?;
        let m = Migrator::new(Path::new("./migrations")).await?;
        m.run(&pool).await?;
        Ok(SqliteQueueHub { pool })
    }

    async fn get_queue_id<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        queue_name: &QueueName,
    ) -> Result<Option<i64>> {
        let q_name: &str = queue_name.as_ref();
        let res =
            sqlx::query_scalar!("SELECT id FROM queues WHERE name = ?", q_name)
                .fetch_optional(tx)
                .await?;
        Ok(res)
    }

    async fn get_consumer_msg_id<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        queue_id: i64,
        consumer: &Consumer,
    ) -> Result<Option<ConsumerMsgId>> {
        let res = sqlx::query_as!(
            ConsumerMsgId,
            "SELECT id AS consumer_id, committed_msg_id FROM consumers
                 WHERE queue_id = ? AND consumer = ?",
            queue_id,
            consumer.0
        )
        .fetch_optional(tx)
        .await?;
        Ok(res)
    }
}

#[async_trait]
impl QueueHub for SqliteQueueHub {
    type Position = MsgPk;

    async fn create_queue(
        &self,
        queue_name: QueueName,
    ) -> Result<CreateQueueResult> {
        use CreateQueueResult::*;

        let mut tx = self.pool.begin().await?;
        let name: &str = queue_name.as_ref();
        let res = sqlx::query!("INSERT INTO queues (name) VALUES (?)", name)
            .execute(&mut tx)
            .await;
        tx.commit().await?;
        match res {
            Ok(_) => Ok(Done),
            Err(Error::Database(err)) if has_code(err.as_ref(), "2067") => {
                Ok(QueueAlreadyExists)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn delete_queue(
        &self,
        queue_name: &QueueName,
    ) -> Result<DeleteQueueResult> {
        use DeleteQueueResult::*;

        let mut tx = self.pool.begin().await?;
        let name: &str = queue_name.as_ref();
        let res = sqlx::query!("DELETE FROM queues where name = ?", name)
            .execute(&mut tx)
            .await;
        tx.commit().await?;
        match res {
            Ok(query_res) => Ok(if query_res.rows_affected() == 1 {
                Done
            } else {
                QueueDoesNotExist
            }),
            Err(err) => Err(err.into()),
        }
    }

    async fn add_consumer(
        &self,
        queue_name: &QueueName,
        consumer: Consumer,
    ) -> Result<AddConsumerResult> {
        use AddConsumerResult::*;

        let mut tx = self.pool.begin().await?;
        let queue_id = match self.get_queue_id(&mut tx, queue_name).await? {
            Some(id) => id,
            None => return Ok(QueueDoesNotExist),
        };
        let res = sqlx::query!(
            "INSERT INTO consumers
             (queue_id, consumer, committed_msg_id) VALUES
             (?, ?, -1)",
            queue_id,
            consumer.0
        )
        .execute(&mut tx)
        .await;
        tx.commit().await?;
        match res {
            Ok(_) => Ok(Done),
            Err(Error::Database(err)) if has_code(err.as_ref(), "2067") => {
                Ok(ConsumerAlreadyAdded)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn remove_consumer(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
    ) -> Result<RemoveConsumerResult> {
        use RemoveConsumerResult::*;

        let mut tx = self.pool.begin().await?;
        let queue_id = match self.get_queue_id(&mut tx, queue_name).await? {
            Some(id) => id,
            None => return Ok(QueueDoesNotExist),
        };
        let res = sqlx::query!(
            "DELETE FROM consumers where queue_id = ? AND consumer = ?",
            queue_id,
            consumer.0
        )
        .execute(&mut tx)
        .await;
        tx.commit().await?;
        match res {
            Ok(query_res) => Ok(if query_res.rows_affected() == 1 {
                Done
            } else {
                UnknownConsumer
            }),
            Err(err) => Err(err.into()),
        }
    }

    async fn push(
        &self,
        queue_name: &QueueName,
        batch: Payloads,
    ) -> Result<PushMessagesResult> {
        use PushMessagesResult::*;

        let mut tx = self.pool.begin().await?;
        let queue_id = match self.get_queue_id(&mut tx, queue_name).await? {
            Some(id) => id,
            None => return Ok(QueueDoesNotExist),
        };
        for payload in batch {
            sqlx::query!(
                "INSERT INTO messages (queue_id, payload) VALUES (?, ?)",
                queue_id,
                payload.0
            )
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        Ok(Done)
    }

    async fn read(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self::Position>> {
        use ReadMessagesResult::*;

        let mut tx = self.pool.begin().await?;
        let queue_id = match self.get_queue_id(&mut tx, queue_name).await? {
            Some(id) => id,
            None => return Ok(QueueDoesNotExist),
        };
        let consumer_msg_id = self
            .get_consumer_msg_id(&mut tx, queue_id, consumer)
            .await?;
        let committed_msg_id = match consumer_msg_id {
            Some(rec) => rec.committed_msg_id,
            None => return Ok(UnknownConsumer),
        };

        let num = number as i64;
        let records = sqlx::query!(
            "SELECT id, payload FROM messages
                 WHERE queue_id = ? AND id > ? LIMIT ?",
            queue_id,
            committed_msg_id,
            num
        )
        .fetch_all(&mut tx)
        .await?;
        tx.commit().await?;
        let messages = records
            .into_iter()
            .map(|r| Message {
                position: MsgPk(r.id),
                payload: Payload(r.payload),
            })
            .collect();
        Ok(Messages(messages))
    }

    async fn commit(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        position: &Self::Position,
    ) -> Result<CommitMessagesResult> {
        use CommitMessagesResult::*;

        let mut tx = self.pool.begin().await?;
        let queue_id = match self.get_queue_id(&mut tx, queue_name).await? {
            Some(id) => id,
            None => return Ok(QueueDoesNotExist),
        };
        let consumer_msg_id = self
            .get_consumer_msg_id(&mut tx, queue_id, consumer)
            .await?;
        let consumer_msg_id = match consumer_msg_id {
            Some(rec) => rec,
            None => return Ok(UnknownConsumer),
        };
        if position.0 < consumer_msg_id.committed_msg_id {
            return Ok(PositionIsOutOfQueue);
        }
        let committed = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM messages
             WHERE queue_id = ? AND id > ? AND id <= ?",
            queue_id,
            consumer_msg_id.committed_msg_id,
            position.0
        )
        .fetch_one(&mut tx)
        .await?;
        if committed == 0 {
            return Ok(PositionIsOutOfQueue);
        }
        sqlx::query!(
            "UPDATE consumers SET committed_msg_id = ? WHERE id = ?",
            position.0,
            consumer_msg_id.consumer_id
        )
        .execute(&mut tx)
        .await?;
        tx.commit().await?;
        Ok(Committed(committed as usize))
    }

    async fn take(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self::Position>> {
        use ReadMessagesResult::*;

        let mut tx = self.pool.begin().await?;
        let queue_id = match self.get_queue_id(&mut tx, queue_name).await? {
            Some(id) => id,
            None => return Ok(QueueDoesNotExist),
        };
        let consumer_msg_id = self
            .get_consumer_msg_id(&mut tx, queue_id, consumer)
            .await?;
        let consumer_msg_id = match consumer_msg_id {
            Some(rec) => rec,
            None => return Ok(UnknownConsumer),
        };

        let num = number as i64;
        let records = sqlx::query!(
            "SELECT id, payload FROM messages
                 WHERE queue_id = ? AND id > ? LIMIT ?",
            queue_id,
            consumer_msg_id.committed_msg_id,
            num
        )
        .fetch_all(&mut tx)
        .await?;
        if let Some(msg_rec) = records.last() {
            sqlx::query!(
                "UPDATE consumers SET committed_msg_id = ? WHERE id = ?",
                msg_rec.id,
                consumer_msg_id.consumer_id
            )
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        let messages = records
            .into_iter()
            .map(|r| Message {
                position: MsgPk(r.id),
                payload: Payload(r.payload),
            })
            .collect();
        Ok(Messages(messages))
    }

    async fn collect_garbage(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let records = sqlx::query_as::<_, QueueMsgId>(
            "SELECT queue_id, MIN(committed_msg_id) AS min_committed_msg_id
             FROM consumers GROUP BY queue_id",
        )
        .fetch_all(&mut tx)
        .await?;

        for r in records {
            sqlx::query!(
                "DELETE FROM messages WHERE queue_id = ? AND id <= ?",
                r.queue_id,
                r.min_committed_msg_id
            )
            .execute(&mut tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn queue_names(&self) -> Result<Vec<QueueName>> {
        let mut conn = self.pool.acquire().await?;
        let res = sqlx::query_scalar!("SELECT name FROM queues")
            .fetch_all(&mut conn)
            .await?;
        let names = res.into_iter().map(QueueName::new).collect();
        Ok(names)
    }

    async fn consumers(
        &self,
        queue_name: &QueueName,
    ) -> Result<GetConsumersResult> {
        use GetConsumersResult::*;

        let mut tx = self.pool.begin().await?;
        let queue_id = match self.get_queue_id(&mut tx, queue_name).await? {
            Some(id) => id,
            None => return Ok(QueueDoesNotExist),
        };

        let res = sqlx::query_scalar!(
            "SELECT consumer FROM consumers where queue_id = ?",
            queue_id
        )
        .fetch_all(&mut tx)
        .await?;
        tx.commit().await?;
        let consumers = res.into_iter().map(Consumer).collect();
        Ok(Consumers(consumers))
    }

    async fn stats(&self, queue_name_prefix: &QueueName) -> Result<Stats> {
        let mut res = HashMap::new();
        let mut conn = self.pool.acquire().await?;
        let q_prefix: &str = queue_name_prefix.as_ref();
        let pattern = format!("{}%", q_prefix);
        let records = sqlx::query_as::<_, StatsRecord>(
            "SELECT
                 Q.name AS queue_name, C.count AS consumers,
                 M.count AS size,
                 C.min_msg_count AS min_msg_count,
                 C.max_msg_count AS max_msg_count
             FROM queues AS Q

             LEFT JOIN (
                 SELECT
                     queue_id, COUNT(*) AS count,
                     MIN(msg_count) AS min_msg_count, MAX(msg_count) AS max_msg_COUNT
                 FROM (
                     SELECT queue_id,
                     (
                         SELECT COUNT(*) FROM messages
                         WHERE queue_id = C.queue_id AND id > C.committed_msg_id
                     ) AS msg_count
                     FROM consumers AS C
                 ) AS C
                 GROUP BY C.queue_id
             ) AS C
             ON Q.id = C.queue_id

             LEFT JOIN (
                 SELECT queue_id, COUNT(*) AS count
                 FROM messages AS M
                 GROUP BY M.queue_id
             ) AS M
             ON Q.id = M.queue_id

             WHERE Q.name LIKE ?",
        )
        .bind(pattern)
        .fetch_all(&mut conn)
        .await?;
        for r in records {
            let q_stats = QueueStats {
                size: r.size.unwrap_or(0) as usize,
                consumers: r.consumers.unwrap_or(0) as usize,
                min_unconsumed_size: r.min_msg_count.unwrap_or(0) as usize,
                max_unconsumed_size: r.max_msg_count.unwrap_or(0) as usize,
            };
            res.insert(QueueName::new(r.queue_name), q_stats);
        }
        Ok(res)
    }
}

fn has_code(error: &dyn DatabaseError, code: &str) -> bool {
    error.code().as_ref().map(|s| s.borrow()) == Some(code)
}
