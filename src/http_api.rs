use crate::queue::QueueHub;
use async_std::io::Result as IoResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tide::{Body, Request, Response, Result as TResult, Route, StatusCode};

type Req = Request<QueueHub>;
type Resp = TResult<Response>;

#[derive(Deserialize)]
struct PushMessage {
    payload: String,
}

async fn push_message(mut req: Req) -> TResult<StatusCode> {
    use crate::queue::PushMessageResult::*;

    let PushMessage { payload } = req.body_json().await?;
    let queue_name = req.param("queue_name")?;
    let res = req.state().push(queue_name, payload.into()).await;
    match res {
        Done => Ok(StatusCode::NoContent),
        QueueDoesNotExist => Ok(StatusCode::NotFound),
    }
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum TakeMessage {
    Empty,
    Message { payload: String },
}

async fn take_message(req: Req) -> Resp {
    use crate::queue::TakeMessageResult::*;

    let queue_name = req.param("queue_name")?;
    Ok(match req.state().take(queue_name).await {
        QueueDoesNotExist => Response::builder(StatusCode::NotFound),
        res => {
            let body = match res {
                QueueDoesNotExist => unreachable!(),
                Message { payload } => TakeMessage::Message {
                    payload: String::from_utf8_lossy(&payload).into(),
                },
                QueueIsEmpty => TakeMessage::Empty,
            };

            Response::builder(StatusCode::Ok).body(Body::from_json(&body)?)
        }
    }
    .build())
}

fn add_messaging_endpoints(mut route: Route<QueueHub>) {
    route.at("/:queue_name/push").post(push_message);
    route.at("/:queue_name/take").post(take_message);
}

#[derive(Deserialize)]
struct CreateQueue {
    queue_name: String,
}

async fn create_queue(mut req: Req) -> Resp {
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

async fn delete_queue(req: Req) -> TResult<StatusCode> {
    use crate::queue::DeleteQueueResult::*;

    let queue_name = req.param("queue_name")?;
    let res = req.state().delete_queue(&queue_name).await;
    match res {
        Done => Ok(StatusCode::NoContent),
        DoesNotExist => Ok(StatusCode::NotFound),
    }
}

async fn reset_queue(req: Req) -> TResult<StatusCode> {
    use crate::queue::ResetQueueResult::*;

    let res = req.state().reset(req.param("queue_name")?).await;
    match res {
        Done => Ok(StatusCode::NoContent),
        DoesNotExist => Ok(StatusCode::NotFound),
    }
}

fn add_queue_endpoints(mut route: Route<QueueHub>) {
    route.at("/create").post(create_queue);
    route.at("/:queue_name").delete(delete_queue);
    route.at("/:queue_name/reset").post(reset_queue);
}

#[derive(Deserialize)]
struct StatsParams {
    #[serde(default)]
    queue_name_prefix: String,
}

#[derive(Serialize)]
struct Stats {
    size: HashMap<String, usize>,
}

async fn stats(req: Req) -> TResult<Body> {
    let q = req.state();
    let params: StatsParams = req.query()?;
    let response = Stats {
        size: q.size(&params.queue_name_prefix).await,
    };
    Body::from_json(&response)
}

pub async fn start_http() -> IoResult<()> {
    tide::log::start();
    let state = QueueHub::new();
    let mut app = tide::with_state(state.clone());
    app.at("/").get(|_| async { Ok("This is Flume") });
    add_messaging_endpoints(app.at("/messaging"));
    add_queue_endpoints(app.at("/queue"));
    app.at("/stats").get(stats);
    app.listen("127.0.0.1:8080").await?;
    Ok(())
}
