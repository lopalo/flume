use crate::{
    broadcaster::Broadcaster,
    queue::{self, Consumer, Messages, Payload, QueueHub, QueueName},
};
use async_std::{io::Result as IoResult, net::SocketAddr};
use serde::{Deserialize, Serialize};
use tide::{
    sse::{self, Sender as SseSender},
    Body, Request, Response, Result as TResult, Route, StatusCode,
};

#[derive(Clone)]
pub struct State<QH: QueueHub> {
    queue_hub: QH,
    broadcaster: Broadcaster,
}

impl<QH: QueueHub> State<QH> {
    pub fn new(queue_hub: QH, broadcaster: Broadcaster) -> Self {
        Self {
            queue_hub,
            broadcaster,
        }
    }
}

type Req<QH> = Request<State<QH>>;
type Resp = TResult<Response>;

#[derive(Deserialize)]
#[serde(bound = "")]
struct PushMessages<QH: QueueHub> {
    batch: Vec<Payload<QH>>,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum PushMessagesResponse {
    NotEnoughSpace,
    Ok,
}

async fn push_messages<QH: QueueHub>(mut req: Req<QH>) -> Resp {
    use crate::queue::PushMessagesResult::*;

    let queue_name = get_queue_name(&req)?;
    let PushMessages { batch } = req.body_json().await?;
    let State {
        queue_hub,
        broadcaster,
    } = req.state();
    broadcaster
        .with_publisher(&queue_name.clone(), |mut publisher| {
            let queue_hub = queue_hub.clone();
            Box::pin(async move {
                match queue_hub.push(&queue_name, &batch).await? {
                    QueueDoesNotExist => not_found(),
                    Done(positions) => {
                        for (position, payload) in positions.iter().zip(batch) {
                            publisher.publish(position, &payload)
                        }
                        json_response(PushMessagesResponse::Ok)
                    }
                    NoSpaceInQueue => {
                        json_response(PushMessagesResponse::NotEnoughSpace)
                    }
                }
            })
        })
        .await
}

#[derive(Deserialize)]
struct ReadMessages {
    consumer: Consumer,
    number: usize,
}

#[derive(Serialize)]
#[serde(tag = "status", bound = "")]
enum ReadMessagesResponse<QH: QueueHub> {
    Messages { batch: Messages<QH> },
    UnknownConsumer,
}

async fn read_messages<QH: QueueHub>(req: Req<QH>) -> Resp {
    use crate::queue::ReadMessagesResult::*;

    let queue_name = get_queue_name(&req)?;
    let ReadMessages { consumer, number } = req.query()?;
    match req
        .state()
        .queue_hub
        .read(&queue_name, &consumer, number)
        .await?
    {
        QueueDoesNotExist => not_found(),
        UnknownConsumer => {
            json_response(ReadMessagesResponse::UnknownConsumer::<QH>)
        }

        Messages(batch) => {
            json_response(ReadMessagesResponse::Messages { batch })
        }
    }
}

#[derive(Deserialize)]
struct CommitMessages<QH: QueueHub> {
    consumer: Consumer,
    position: QH::Position,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum CommitMessagesResponse {
    Committed { number: usize },
    UnknownConsumer,
    PositionIsOutOfQueue,
}

async fn commit_messages<QH: QueueHub>(mut req: Req<QH>) -> Resp {
    use crate::queue::CommitMessagesResult::*;

    let queue_name = get_queue_name(&req)?;
    let params: CommitMessages<QH> = req.body_form().await?;
    let CommitMessages { consumer, position } = params;
    match req
        .state()
        .queue_hub
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

async fn take_messages<QH: QueueHub>(mut req: Req<QH>) -> Resp {
    use crate::queue::ReadMessagesResult::*;

    let queue_name = get_queue_name(&req)?;
    let ReadMessages { consumer, number } = req.body_form().await?;
    match req
        .state()
        .queue_hub
        .take(&queue_name, &consumer, number)
        .await?
    {
        QueueDoesNotExist => not_found(),
        UnknownConsumer => {
            json_response(ReadMessagesResponse::UnknownConsumer::<QH>)
        }

        Messages(batch) => {
            json_response(ReadMessagesResponse::Messages { batch })
        }
    }
}

async fn listen_to_messages<QH: QueueHub>(
    req: Req<QH>,
    sender: SseSender,
) -> TResult<()> {
    let queue_name = get_queue_name(&req)?;
    req.state().broadcaster.listen(sender, &queue_name).await;
    Ok(())
}

fn add_messaging_endpoints<QH: QueueHub>(mut route: Route<State<QH>>) {
    route.at("/:queue_name/push").post(push_messages);
    route.at("/:queue_name/read").get(read_messages);
    route.at("/:queue_name/commit").post(commit_messages);
    route.at("/:queue_name/take").post(take_messages);
    route
        .at("/:queue_name/listen")
        .get(sse::endpoint(listen_to_messages));
}

#[derive(Deserialize)]
struct CreateQueue {
    queue_name: QueueName,
}

async fn create_queue<QH: QueueHub>(mut req: Req<QH>) -> Resp {
    use crate::queue::CreateQueueResult::*;

    let CreateQueue { queue_name } = req.body_form().await?;
    let State {
        queue_hub,
        broadcaster,
    } = req.state();
    let res = queue_hub.create_queue(queue_name.clone()).await?;
    Ok(match res {
        Done => {
            broadcaster.create_channel(queue_name).await;
            Response::builder(StatusCode::Created)
        }
        QueueAlreadyExists => {
            Response::builder(StatusCode::Conflict).body("queue already exists")
        }
    }
    .build())
}

async fn delete_queue<QH: QueueHub>(req: Req<QH>) -> TResult<StatusCode> {
    use crate::queue::DeleteQueueResult::*;

    let queue_name = get_queue_name(&req)?;
    let State {
        queue_hub,
        broadcaster,
    } = req.state();
    let res = queue_hub.delete_queue(&queue_name).await?;
    match res {
        Done => {
            broadcaster.delete_channel(&queue_name).await;
            Ok(StatusCode::NoContent)
        }
        QueueDoesNotExist => Ok(StatusCode::NotFound),
    }
}

async fn add_consumer<QH: QueueHub>(req: Req<QH>) -> Resp {
    use crate::queue::AddConsumerResult::*;

    let queue_name = get_queue_name(&req)?;
    let consumer = req
        .param("consumer")
        .map(str::to_owned)
        .map(Consumer::new)?;
    let res = req
        .state()
        .queue_hub
        .add_consumer(&queue_name, consumer)
        .await?;
    Ok(match res {
        QueueDoesNotExist => Response::builder(StatusCode::NotFound),
        ConsumerAlreadyAdded => Response::builder(StatusCode::Conflict)
            .body("consumer already added"),
        Done => Response::builder(StatusCode::Created),
    }
    .build())
}

async fn remove_consumer<QH: QueueHub>(req: Req<QH>) -> Resp {
    use crate::queue::RemoveConsumerResult::*;

    let queue_name = get_queue_name(&req)?;
    let consumer = req
        .param("consumer")
        .map(str::to_owned)
        .map(Consumer::new)?;
    let res = req
        .state()
        .queue_hub
        .remove_consumer(&queue_name, &consumer)
        .await?;
    Ok(match res {
        QueueDoesNotExist => {
            Response::builder(StatusCode::NotFound).body("unknown queue")
        }
        UnknownConsumer => {
            Response::builder(StatusCode::NotFound).body("unknown consumer")
        }
        Done => Response::builder(StatusCode::Ok),
    }
    .build())
}

#[derive(Serialize)]
struct QueueListResponse {
    queue_names: Vec<QueueName>,
}

async fn queue_list<QH: QueueHub>(req: Req<QH>) -> TResult<Body> {
    let queue_names = req.state().queue_hub.queue_names().await?;
    Body::from_json(&QueueListResponse { queue_names })
}

#[derive(Serialize)]
struct GetConsumersResponse {
    consumers: Vec<Consumer>,
}

async fn queue_consumers<QH: QueueHub>(req: Req<QH>) -> Resp {
    use crate::queue::GetConsumersResult::*;

    let queue_name = get_queue_name(&req)?;
    match req.state().queue_hub.consumers(&queue_name).await? {
        QueueDoesNotExist => not_found(),
        Consumers(consumers) => {
            json_response(GetConsumersResponse { consumers })
        }
    }
}

fn add_queue_endpoints<QH: QueueHub>(mut route: Route<State<QH>>) {
    route.at("/list").get(queue_list);
    route.at("/create").post(create_queue);
    route.at("/:queue_name").delete(delete_queue);
    route.at("/:queue_name/consumers").get(queue_consumers);
    route
        .at("/:queue_name/consumer/:consumer")
        .put(add_consumer);
    route
        .at("/:queue_name/consumer/:consumer")
        .delete(remove_consumer);
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

async fn get_stats<QH: QueueHub>(req: Req<QH>) -> TResult<Body> {
    let qh = req.state();
    let Stats { queue_name_prefix } = req.query()?;
    let response = StatsResponse {
        stats: qh.queue_hub.stats(&queue_name_prefix).await?,
    };
    Body::from_json(&response)
}

pub async fn start_http<QH>(
    state: State<QH>,
    socket_addr: SocketAddr,
) -> IoResult<()>
where
    QH: QueueHub,
{
    let mut app = tide::with_state(state);
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
