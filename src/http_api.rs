use crate::queue::QueueHub;
use async_std::{io::Result as IoResult, net::SocketAddr};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tide::{Body, Request, Response, Result as TResult, Route, StatusCode};

type Resp = TResult<Response>;

#[derive(Deserialize)]
struct PushMessages {
    batch: Vec<String>,
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
    let queue_name = req.param("queue_name")?;
    let batch = batch.into_iter().map(|payload| payload.into()).collect();
    Ok(match req.state().push(queue_name, batch).await {
        QueueDoesNotExist => Response::builder(StatusCode::NotFound),
        res => {
            let body = match res {
                QueueDoesNotExist => unreachable!(),
                Done => PushMessagesResponse::Ok,
                NoSpaceInQueue => PushMessagesResponse::NotEnoughSpace,
            };
            Response::builder(StatusCode::Ok).body(Body::from_json(&body)?)
        }
    }
    .build())
}

#[derive(Deserialize)]
struct TakeMessages {
    number: usize,
}

#[derive(Serialize)]
struct TakeMessagesResponse {
    batch: Vec<String>,
}

async fn take_messages<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::TakeMessagesResult::*;

    let TakeMessages { number } = req.body_json().await?;
    let queue_name = req.param("queue_name")?;
    Ok(match req.state().take(queue_name, number).await {
        QueueDoesNotExist => Response::builder(StatusCode::NotFound),

        Messages { batch } => {
            let batch = batch
                .into_iter()
                .map(|payload| String::from_utf8_lossy(&payload).into())
                .collect();
            let body = TakeMessagesResponse { batch };
            Response::builder(StatusCode::Ok).body(Body::from_json(&body)?)
        }
    }
    .build())
}

fn add_messaging_endpoints<S: QueueHub>(mut route: Route<S>) {
    //TODO: endpoints for binary payload
    route.at("/:queue_name/push").post(push_messages);
    route.at("/:queue_name/take").post(take_messages);
}

#[derive(Deserialize)]
struct CreateQueue {
    queue_name: String,
}

async fn create_queue<S: QueueHub>(mut req: Request<S>) -> Resp {
    use crate::queue::CreateQueueResult::*;

    let CreateQueue { queue_name } = req.body_json().await?;
    let res = req.state().create_queue(&queue_name).await;
    Ok(match res {
        Done => Response::builder(StatusCode::Created),
        AlreadyExists => {
            Response::builder(StatusCode::Conflict).body("queue already exists")
        }
    }
    .build())
}

async fn delete_queue<S: QueueHub>(req: Request<S>) -> TResult<StatusCode> {
    use crate::queue::DeleteQueueResult::*;

    let queue_name = req.param("queue_name")?;
    let res = req.state().delete_queue(&queue_name).await;
    match res {
        Done => Ok(StatusCode::NoContent),
        DoesNotExist => Ok(StatusCode::NotFound),
    }
}

async fn reset_queue<S: QueueHub>(req: Request<S>) -> TResult<StatusCode> {
    use crate::queue::ResetQueueResult::*;

    let res = req.state().reset(req.param("queue_name")?).await;
    match res {
        Done => Ok(StatusCode::NoContent),
        DoesNotExist => Ok(StatusCode::NotFound),
    }
}

fn add_queue_endpoints<S: QueueHub>(mut route: Route<S>) {
    route.at("/create").post(create_queue);
    route.at("/:queue_name").delete(delete_queue);
    route.at("/:queue_name/reset").post(reset_queue);
}

#[derive(Deserialize)]
struct Stats {
    #[serde(default)]
    queue_name_prefix: String,
}

#[derive(Serialize)]
struct StatsResponse {
    size: HashMap<String, usize>,
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
