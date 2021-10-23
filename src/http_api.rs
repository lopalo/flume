use crate::queue::Queue;
use async_std::io::Result as IoResult;
use serde::{Deserialize, Serialize};
use tide::{Body, Request, Result as TResult, Server, StatusCode};

type Req = Request<Queue>;

#[derive(Deserialize)]
struct PushMessage {
    payload: String,
}

async fn push_message(mut req: Req) -> TResult<StatusCode> {
    let body: PushMessage = req.body_json().await?;
    req.state().push(body.payload.into()).await;
    Ok(StatusCode::Created)
}

#[derive(Serialize)]
enum TakeMessage {
    Empty,
    Message { payload: String },
}

async fn take_message(req: Req) -> TResult<Body> {
    use TakeMessage::*;
    let response = match req.state().take().await {
        None => Empty,
        Some(p) => Message {
            payload: String::from_utf8_lossy(&p).into(),
        },
    };
    Body::from_json(&response)
}

fn make_messaging_routes(state: Queue) -> Server<Queue> {
    let mut api = tide::with_state(state);
    api.at("/push").post(push_message);
    api.at("/take").post(take_message);
    api
}

async fn reset_queue(req: Req) -> TResult<StatusCode> {
    req.state().reset().await;
    Ok(StatusCode::NoContent)
}

fn make_management_routes(state: Queue) -> Server<Queue> {
    let mut api = tide::with_state(state);
    api.at("/reset_queue").post(reset_queue);
    api
}

#[derive(Serialize)]
struct Stats {
    size: usize,
}

async fn stats(req: Req) -> TResult<Body> {
    let q = req.state();
    let response = Stats {
        size: q.size().await,
    };
    Body::from_json(&response)
}

pub async fn start_http() -> IoResult<()> {
    tide::log::start();
    let state = Queue::new();
    let mut app = tide::with_state(state.clone());
    app.at("/").get(|_| async { Ok("This is Flume") });
    app.at("/messaging")
        .nest(make_messaging_routes(state.clone()));
    app.at("/management")
        .nest(make_management_routes(state.clone()));
    app.at("/stats").get(stats);
    app.listen("127.0.0.1:8080").await?;
    Ok(())
}
