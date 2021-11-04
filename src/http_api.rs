use crate::queue::{Consumer, Messages, Payloads, QueueHub, QueueName};
use async_std::{io::Result as IoResult, net::SocketAddr};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

    let PushMessages { batch } = req.body_json().await?;
    let queue_name = get_queue_name(&req)?;
    match req.state().push(&queue_name, batch).await {
        QueueDoesNotExist => not_found(),
        Done => json_response(PushMessagesResponse::Ok),
        NoSpaceInQueue => json_response(PushMessagesResponse::NotEnoughSpace),
    }
}

#[derive(Deserialize)]
struct TakeMessages {
    consumer: Consumer,
    number: usize,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum TakeMessagesResponse<Pos>
where
    Pos: Serialize,
{
    Messages { batch: Messages<Pos> },
    UnknownConsumer,
}

async fn take_messages<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::ReadMessagesResult::*;

    let TakeMessages { consumer, number } = req.body_json().await?;
    let queue_name = get_queue_name(&req)?;
    match req.state().take(&queue_name, &consumer, number).await {
        QueueDoesNotExist => not_found(),
        UnknownConsumer => {
            json_response(TakeMessagesResponse::UnknownConsumer::<S::Position>)
        }

        Messages(batch) => {
            json_response(TakeMessagesResponse::Messages { batch })
        }
    }
}

fn add_messaging_endpoints<S: QueueHub>(mut route: Route<S>) {
    route.at("/:queue_name/push").post(push_messages);
    route.at("/:queue_name/take").post(take_messages);
}

#[derive(Deserialize)]
struct CreateQueue {
    queue_name: QueueName,
}

async fn create_queue<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::CreateQueueResult::*;

    let CreateQueue { queue_name } = req.body_json().await?;
    let res = req.state().create_queue(queue_name).await;
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
    let res = req.state().delete_queue(&queue_name).await;
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

    let AddConsumer { consumer } = req.body_json().await?;
    let res = req
        .state()
        .add_consumer(&get_queue_name(&req)?, consumer)
        .await;
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

    let RemoveConsumer { consumer } = req.body_json().await?;
    let res = req
        .state()
        .remove_consumer(&get_queue_name(&req)?, &consumer)
        .await;
    match res {
        QueueDoesNotExist => not_found(),
        UnknownConsumer => json_response(RemoveConsumerResult::UnknownConsumer),
        Done => json_response(RemoveConsumerResult::Ok),
    }
}

fn add_queue_endpoints<S: QueueHub>(mut route: Route<S>) {
    route.at("/create").post(create_queue);
    route.at("/:queue_name").delete(delete_queue);
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
    size: HashMap<QueueName, usize>,
}

async fn stats<S: QueueHub>(req: Request<S>) -> TResult<Body> {
    let qh = req.state();
    let params: Stats = req.query()?;
    let response = StatsResponse {
        size: qh.size(&params.queue_name_prefix).await,
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
    app.at("/stats").get(stats);
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
