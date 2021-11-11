use crate::queue::{self, Consumer, Messages, Payloads, QueueHub, QueueName};
use async_std::{io::Result as IoResult, net::SocketAddr};
use serde::{Deserialize, Serialize};
use tide::{Body, Request, Response, Result as TResult, Route, StatusCode};

type Resp = TResult<Response>;

#[derive(Deserialize)]
struct PushMessages {
    batch: Payloads,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum PushMessagesResponse {
    NotEnoughSpace,
    Ok,
}

async fn push_messages<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::PushMessagesResult::*;

    let queue_name = get_queue_name(&req)?;
    let PushMessages { batch } = req.body_json().await?;
    match req.state().push(&queue_name, batch).await? {
        QueueDoesNotExist => not_found(),
        Done => json_response(PushMessagesResponse::Ok),
        NoSpaceInQueue => json_response(PushMessagesResponse::NotEnoughSpace),
    }
}

#[derive(Deserialize)]
struct ReadMessages {
    consumer: Consumer,
    number: usize,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum ReadMessagesResponse<Pos>
where
    Pos: Serialize,
{
    Messages { batch: Messages<Pos> },
    UnknownConsumer,
}

async fn read_messages<S: QueueHub>(req: Request<S>) -> Resp {
    use crate::queue::ReadMessagesResult::*;

    let queue_name = get_queue_name(&req)?;
    let ReadMessages { consumer, number } = req.query()?;
    match req.state().read(&queue_name, &consumer, number).await? {
        QueueDoesNotExist => not_found(),
        UnknownConsumer => {
            json_response(ReadMessagesResponse::UnknownConsumer::<S::Position>)
        }

        Messages(batch) => {
            json_response(ReadMessagesResponse::Messages { batch })
        }
    }
}

#[derive(Deserialize)]
struct CommitMessages<Pos> {
    consumer: Consumer,
    position: Pos,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum CommitMessagesResponse {
    Committed { number: usize },
    UnknownConsumer,
    PositionIsOutOfQueue,
}

async fn commit_messages<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::CommitMessagesResult::*;

    let queue_name = get_queue_name(&req)?;
    let params: CommitMessages<S::Position> = req.body_form().await?;
    let CommitMessages { consumer, position } = params;
    match req
        .state()
        .commit(&queue_name, &consumer, &position)
        .await?
    {
        QueueDoesNotExist => not_found(),
        UnknownConsumer => {
            json_response(CommitMessagesResponse::UnknownConsumer)
        }
        PositionIsOutOfQueue => {
            json_response(CommitMessagesResponse::PositionIsOutOfQueue)
        }
        Committed(number) => {
            json_response(CommitMessagesResponse::Committed { number })
        }
    }
}

async fn take_messages<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::ReadMessagesResult::*;

    let queue_name = get_queue_name(&req)?;
    let ReadMessages { consumer, number } = req.body_form().await?;
    match req.state().take(&queue_name, &consumer, number).await? {
        QueueDoesNotExist => not_found(),
        UnknownConsumer => {
            json_response(ReadMessagesResponse::UnknownConsumer::<S::Position>)
        }

        Messages(batch) => {
            json_response(ReadMessagesResponse::Messages { batch })
        }
    }
}

fn add_messaging_endpoints<S: QueueHub>(mut route: Route<S>) {
    route.at("/:queue_name/push").post(push_messages);
    route.at("/:queue_name/read").get(read_messages);
    route.at("/:queue_name/commit").post(commit_messages);
    route.at("/:queue_name/take").post(take_messages);
    //TODO: SSE for streaming messages
}

#[derive(Deserialize)]
struct CreateQueue {
    queue_name: QueueName,
}

async fn create_queue<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::CreateQueueResult::*;

    let CreateQueue { queue_name } = req.body_form().await?;
    let res = req.state().create_queue(queue_name).await?;
    Ok(match res {
        Done => Response::builder(StatusCode::Created),
        QueueAlreadyExists => {
            Response::builder(StatusCode::Conflict).body("queue already exists")
        }
    }
    .build())
}

async fn delete_queue<S: QueueHub>(req: Request<S>) -> TResult<StatusCode> {
    use crate::queue::DeleteQueueResult::*;

    let queue_name = get_queue_name(&req)?;
    let res = req.state().delete_queue(&queue_name).await?;
    match res {
        Done => Ok(StatusCode::NoContent),
        QueueDoesNotExist => Ok(StatusCode::NotFound),
    }
}

#[derive(Deserialize)]
struct AddConsumer {
    consumer: Consumer,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum AddConsumerResult {
    Ok,
    ConsumerAlreadyAdded,
}

async fn add_consumer<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::AddConsumerResult::*;

    let queue_name = get_queue_name(&req)?;
    let AddConsumer { consumer } = req.body_form().await?;
    let res = req.state().add_consumer(&queue_name, consumer).await?;
    match res {
        QueueDoesNotExist => not_found(),
        ConsumerAlreadyAdded => {
            json_response(AddConsumerResult::ConsumerAlreadyAdded)
        }
        Done => json_response(AddConsumerResult::Ok),
    }
}

#[derive(Deserialize)]
struct RemoveConsumer {
    consumer: Consumer,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum RemoveConsumerResult {
    Ok,
    UnknownConsumer,
}

async fn remove_consumer<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::RemoveConsumerResult::*;

    let queue_name = get_queue_name(&req)?;
    let RemoveConsumer { consumer } = req.body_form().await?;
    let res = req.state().remove_consumer(&queue_name, &consumer).await?;
    match res {
        QueueDoesNotExist => not_found(),
        UnknownConsumer => json_response(RemoveConsumerResult::UnknownConsumer),
        Done => json_response(RemoveConsumerResult::Ok),
    }
}

#[derive(Serialize)]
struct QueueListResponse {
    queue_names: Vec<QueueName>,
}

async fn queue_list<S: QueueHub>(req: Request<S>) -> TResult<Body> {
    let queue_names = req.state().queue_names().await?;
    Body::from_json(&QueueListResponse { queue_names })
}

#[derive(Serialize)]
struct GetConsumersResponse {
    consumers: Vec<Consumer>,
}

async fn queue_consumers<S: QueueHub>(req: Request<S>) -> Resp {
    use crate::queue::GetConsumersResult::*;

    let queue_name = get_queue_name(&req)?;
    match req.state().consumers(&queue_name).await? {
        QueueDoesNotExist => not_found(),
        Consumers(consumers) => {
            json_response(GetConsumersResponse { consumers })
        }
    }
}

fn add_queue_endpoints<S: QueueHub>(mut route: Route<S>) {
    route.at("/list").get(queue_list);
    route.at("/create").post(create_queue);
    route.at("/:queue_name").delete(delete_queue);
    route.at("/:queue_name/consumers").get(queue_consumers);
    route.at("/:queue_name/add_consumer").post(add_consumer);
    route
        .at("/:queue_name/remove_consumer")
        .post(remove_consumer);
}

#[derive(Deserialize)]
struct Stats {
    #[serde(default)]
    queue_name_prefix: QueueName,
}

#[derive(Serialize)]
struct StatsResponse {
    stats: queue::Stats,
}

async fn get_stats<S: QueueHub>(req: Request<S>) -> TResult<Body> {
    let qh = req.state();
    let Stats { queue_name_prefix } = req.query()?;
    let response = StatsResponse {
        stats: qh.stats(&queue_name_prefix).await?,
    };
    Body::from_json(&response)
}

pub async fn start_http<QH>(
    queue_hub: QH,
    socket_addr: SocketAddr,
) -> IoResult<()>
where
    QH: QueueHub,
{
    let mut app = tide::with_state(queue_hub);
    app.at("/").get(|_| async { Ok("This is Flume") });
    add_messaging_endpoints(app.at("/messaging"));
    add_queue_endpoints(app.at("/queue"));
    app.at("/stats").get(get_stats);
    app.listen(socket_addr).await?;
    Ok(())
}

fn get_queue_name<S>(req: &Request<S>) -> TResult<QueueName> {
    req.param("queue_name")
        .map(str::to_owned)
        .map(QueueName::new)
}

fn not_found() -> Resp {
    Ok(Response::new(StatusCode::NotFound))
}

fn json_response<T>(json: T) -> Resp
where
    T: Serialize,
{
    Ok(Body::from_json(&json)?.into())
}
