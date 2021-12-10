use anyhow::Result;
use async_std::task;
use flume::queue::*;
use futures::future;
use rand::{thread_rng, Rng};
use rstest::rstest;
use sqlx::sqlite::SqliteConnectOptions;
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    str::FromStr,
    time::Duration,
};

fn qname(s: &str) -> QueueName {
    QueueName::new(s.to_owned())
}

fn cons(s: &str) -> Consumer {
    Consumer::new(s.to_owned())
}

fn payload<QH: QueueHub>(s: &str) -> Payload<QH> {
    QH::payload(s.to_owned())
}

fn payloads<QH: QueueHub>(messages: Messages<QH>) -> Vec<Payload<QH>> {
    messages.into_iter().map(|m| m.payload).collect()
}

fn extract_payloads<QH: QueueHub>(
    res: ReadMessagesResult<QH>,
) -> Vec<Payload<QH>> {
    use ReadMessagesResult::*;
    match res {
        Messages(messages) => payloads(messages),
        QueueDoesNotExist => panic!("queue does not exist"),
        UnknownConsumer => panic!("unknown consumer"),
    }
}

fn push_is_ok<QH: QueueHub>(res: PushMessagesResult<QH>) -> bool {
    if let PushMessagesResult::Done(..) = res {
        true
    } else {
        false
    }
}

fn in_memory_qh() -> in_memory::InMemoryQueueHub {
    in_memory::InMemoryQueueHub::new(1000)
}

fn sqlite_qh() -> sqlite::SqliteQueueHub {
    let mut db_path = env::temp_dir();
    db_path.push("flume_integration_test_sqlite");
    fs::create_dir_all(&db_path).unwrap();
    db_path.push(format!("{}.db", thread_rng().gen::<u128>()));
    let url = format!("sqlite://{}", db_path.to_str().unwrap());
    let conn_opts = SqliteConnectOptions::from_str(&url).unwrap();
    task::block_on(sqlite::SqliteQueueHub::connect(conn_opts)).unwrap()
}

fn aof_qh() -> aof::AofQueueHub {
    let mut data_path = env::temp_dir();
    data_path.push("flume_integration_test_aof");
    data_path.push(format!("data_{}", thread_rng().gen::<u128>()));
    fs::create_dir_all(&data_path).unwrap();
    task::block_on(aof::AofQueueHub::load(data_path.into())).unwrap()
}

#[rstest]
#[case::in_memory(in_memory_qh())]
#[case::sqlite(sqlite_qh())]
#[case::aof(aof_qh())]
async fn two_pushes_one_take<QH: QueueHub>(#[case] qh: QH) -> Result<()> {
    let pl = payload::<QH>;
    let qn = qname("foo");
    let c = cons("alpha");

    let res = qh.create_queue(qn.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);
    let res = qh.push(&qn, &[pl("11"), pl("22")]).await?;
    assert!(push_is_ok(res));
    let res = qh.push(&qn, &[pl("x"), pl("y"), pl("z")]).await?;
    assert!(push_is_ok(res));

    let res = qh.add_consumer(&qn, c.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);
    let res = qh.take(&qn, &c, 100).await?;
    assert_eq!(
        extract_payloads(res),
        vec![pl("11"), pl("22"), pl("x"), pl("y"), pl("z")]
    );
    let res = qh.take(&qn, &c, 100).await?;
    assert_eq!(extract_payloads(res), vec![]);
    Ok(())
}

#[rstest]
#[case::in_memory(in_memory_qh())]
#[case::sqlite(sqlite_qh())]
#[case::aof(aof_qh())]
async fn one_push_two_takes<QH: QueueHub>(#[case] qh: QH) -> Result<()> {
    let pl = payload::<QH>;
    let qn = qname("foo");
    let c = cons("alpha");

    let res = qh.create_queue(qn.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);
    let res = qh
        .push(&qn, &[pl("11"), pl("22"), pl("x"), pl("y"), pl("z")])
        .await?;
    assert!(push_is_ok(res));

    let res = qh.add_consumer(&qn, c.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);
    let res = qh.take(&qn, &c, 3).await?;
    assert_eq!(extract_payloads(res), vec![pl("11"), pl("22"), pl("x")]);
    let res = qh.take(&qn, &c, 2).await?;
    assert_eq!(extract_payloads(res), vec![pl("y"), pl("z")]);
    let res = qh.take(&qn, &c, 2).await?;
    assert_eq!(extract_payloads(res), vec![]);
    Ok(())
}

#[rstest]
#[case::in_memory(in_memory_qh())]
#[case::sqlite(sqlite_qh())]
async fn read_and_commit<QH: QueueHub>(#[case] qh: QH) -> Result<()> {
    let pl = payload::<QH>;
    let qn = qname("foo");
    let c = cons("alpha");

    let res = qh.create_queue(qn.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);
    let res = qh.add_consumer(&qn, c.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);
    let res = qh
        .push(&qn, &[pl("11"), pl("22"), pl("x"), pl("y"), pl("z")])
        .await?;
    assert!(push_is_ok(res));

    let res = qh.read(&qn, &c, 10).await?;
    let last_pos = match res {
        ReadMessagesResult::Messages(ref messages) => {
            messages.last().unwrap().position.clone()
        }
        _ => panic!("no messages"),
    };
    assert_eq!(
        extract_payloads(res),
        vec![pl("11"), pl("22"), pl("x"), pl("y"), pl("z")]
    );
    let res = qh.read(&qn, &c, 2).await?;
    assert_eq!(extract_payloads(res), vec![pl("11"), pl("22")]);
    let res = qh.read(&qn, &c, 3).await?;
    let mid_pos = match res {
        ReadMessagesResult::Messages(ref messages) => {
            messages.last().unwrap().position.clone()
        }
        _ => panic!("no messages"),
    };
    assert_eq!(extract_payloads(res), vec![pl("11"), pl("22"), pl("x")]);
    let res = qh.commit(&qn, &c, &mid_pos).await?;
    assert_eq!(res, CommitMessagesResult::Committed(3));

    let res = qh.read(&qn, &c, 5).await?;
    assert_eq!(extract_payloads(res), vec![pl("y"), pl("z")]);
    let res = qh.commit(&qn, &c, &mid_pos).await?;
    assert_eq!(res, CommitMessagesResult::PositionIsOutOfQueue);
    let res = qh.read(&qn, &c, 5).await?;
    assert_eq!(extract_payloads(res), vec![pl("y"), pl("z")]);

    let res = qh.commit(&qn, &c, &last_pos).await?;
    assert_eq!(res, CommitMessagesResult::Committed(2));
    let res = qh.read(&qn, &c, 5).await?;
    assert_eq!(extract_payloads(res), vec![]);

    Ok(())
}

#[rstest]
#[case::in_memory(in_memory_qh())]
#[case::sqlite(sqlite_qh())]
async fn refill_during_gc<QH: QueueHub>(#[case] qh: QH) -> Result<()> {
    let pl = payload::<QH>;
    let qn = qname("foo");
    let c = cons("alpha");

    let res = qh.create_queue(qn.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);
    let res = qh.add_consumer(&qn, c.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);

    qh.push(&qn, &[pl("11"), pl("22")]).await?;

    let res = qh.take(&qn, &c, 10).await?;
    assert_eq!(extract_payloads(res), vec![pl("11"), pl("22")]);
    let res = qh.take(&qn, &c, 10).await?;
    assert_eq!(extract_payloads(res), vec![]);

    qh.collect_garbage().await?;
    qh.push(&qn, &[pl("x"), pl("y"), pl("z")]).await?;
    qh.collect_garbage().await?;

    let res = qh.take(&qn, &c, 10).await?;
    assert_eq!(extract_payloads(res), vec![pl("x"), pl("y"), pl("z")]);
    let res = qh.take(&qn, &c, 10).await?;
    assert_eq!(extract_payloads(res), vec![]);

    Ok(())
}

#[rstest]
#[case::in_memory(in_memory_qh())]
#[case::sqlite(sqlite_qh())]
#[case::aof(aof_qh())]
async fn concurrent_push_take<QH: QueueHub>(#[case] qh: QH) -> Result<()> {
    let pl = payload::<QH>;
    let qn = qname("foo");
    let c = cons("alpha");

    let res = qh.create_queue(qn.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);
    let res = qh.add_consumer(&qn, c.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);

    let chunk_size = 4usize;
    let payloads: Vec<_> = (10u32..90).map(|n| pl(&n.to_string())).collect();
    let chunks: Vec<_> = payloads.chunks(chunk_size).collect();

    future::join_all(chunks.iter().map(|&ch| {
        let payloads = ch.to_owned();
        async {
            let payloads = payloads;
            let mcs = thread_rng().gen_range(0..4);
            task::sleep(Duration::from_micros(mcs)).await;
            qh.push(&qn, &payloads).await
        }
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;

    let mut res: Vec<_> =
        future::join_all((0..chunks.len()).into_iter().map(|_| async {
            let mcs = thread_rng().gen_range(0..4);
            task::sleep(Duration::from_micros(mcs)).await;
            qh.take(&qn, &c, chunk_size).await
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(extract_payloads)
        .collect();

    res.sort_by_key(|b| format!("{:?}", b));
    assert_eq!(res, chunks);

    Ok(())
}

#[rstest]
#[case::in_memory(in_memory_qh())]
#[case::sqlite(sqlite_qh())]
async fn three_consumers_and_gc<QH: QueueHub>(#[case] qh: QH) -> Result<()> {
    let pl = payload::<QH>;
    let qn = qname("foo");
    let alpha = cons("alpha");
    let beta = cons("beta");
    let gamma = cons("gamma");

    let res = qh.create_queue(qn.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);

    qh.push(&qn, &[pl("x"), pl("y"), pl("z")]).await?;
    let res = qh.add_consumer(&qn, alpha.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);

    qh.push(&qn, &[pl("1"), pl("2")]).await?;
    let res = qh.add_consumer(&qn, beta.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);
    let res = qh.take(&qn, &alpha, 2).await?;
    assert_eq!(extract_payloads(res), vec![pl("x"), pl("y")]);

    qh.push(&qn, &[pl("3"), pl("4")]).await?;
    let res = qh.add_consumer(&qn, gamma.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);
    let res = qh.take(&qn, &gamma, 4).await?;
    assert_eq!(
        extract_payloads(res),
        vec![pl("x"), pl("y"), pl("z"), pl("1"),]
    );

    qh.collect_garbage().await?;
    let mut res = qh.stats(&qn).await?;
    assert_eq!(1, res.len());
    assert_eq!(
        QueueStats {
            size: 7,
            consumers: 3,
            min_unconsumed_size: 3,
            max_unconsumed_size: 7
        },
        res.remove(&qn).unwrap()
    );

    let res = qh.take(&qn, &alpha, 2).await?;
    assert_eq!(extract_payloads(res), vec![pl("z"), pl("1"),]);
    let res = qh.take(&qn, &beta, 3).await?;
    assert_eq!(extract_payloads(res), vec![pl("x"), pl("y"), pl("z"),]);

    let mut res = qh.stats(&qn).await?;
    assert_eq!(1, res.len());
    assert_eq!(
        QueueStats {
            size: 7,
            consumers: 3,
            min_unconsumed_size: 3,
            max_unconsumed_size: 4
        },
        res.remove(&qn).unwrap()
    );
    qh.collect_garbage().await?;
    let mut res = qh.stats(&qn).await?;
    assert_eq!(
        QueueStats {
            size: 4,
            consumers: 3,
            min_unconsumed_size: 3,
            max_unconsumed_size: 4
        },
        res.remove(&qn).unwrap()
    );

    let res = qh.take(&qn, &gamma, 100).await?;
    assert_eq!(extract_payloads(res), vec![pl("2"), pl("3"), pl("4"),]);
    let res = qh.remove_consumer(&qn, &beta).await?;
    assert_eq!(res, RemoveConsumerResult::Done);
    let res = qh.read(&qn, &alpha, 100).await?;
    let last_pos = match res {
        ReadMessagesResult::Messages(ref messages) => {
            messages.last().unwrap().position.clone()
        }
        _ => panic!("no messages"),
    };
    assert_eq!(extract_payloads(res), vec![pl("2"), pl("3"), pl("4")]);
    qh.collect_garbage().await?;
    let mut res = qh.stats(&qn).await?;
    assert_eq!(
        QueueStats {
            size: 3,
            consumers: 2,
            min_unconsumed_size: 0,
            max_unconsumed_size: 3
        },
        res.remove(&qn).unwrap()
    );

    let res = qh.commit(&qn, &alpha, &last_pos).await?;
    assert_eq!(res, CommitMessagesResult::Committed(3));

    qh.collect_garbage().await?;
    let mut res = qh.stats(&qn).await?;
    assert_eq!(
        QueueStats {
            size: 0,
            consumers: 2,
            min_unconsumed_size: 0,
            max_unconsumed_size: 0
        },
        res.remove(&qn).unwrap()
    );

    Ok(())
}

#[rstest]
#[case::in_memory(in_memory_qh())]
#[case::sqlite(sqlite_qh())]
async fn two_queues<QH: QueueHub>(#[case] qh: QH) -> Result<()> {
    let pl = payload::<QH>;
    let foo_q = qname("foo");
    let bar_q = qname("bar");
    let alpha = cons("alpha");
    let beta = cons("beta");
    let gamma = cons("gamma");

    let res = qh.create_queue(foo_q.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);

    qh.push(&foo_q, &[pl("x"), pl("y"), pl("z")]).await?;
    let res = qh.add_consumer(&foo_q, alpha.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);

    let res = qh.create_queue(bar_q.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);
    let res = qh.add_consumer(&bar_q, alpha.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);
    qh.push(&bar_q, &[pl("11"), pl("22"), pl("33")]).await?;
    let res = qh.add_consumer(&bar_q, gamma.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);

    let res = qh.add_consumer(&foo_q, beta.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);

    let res = qh.take(&foo_q, &alpha, 2).await?;
    assert_eq!(extract_payloads(res), vec![pl("x"), pl("y"),]);

    let res = qh.read(&bar_q, &alpha, 3).await?;
    assert_eq!(extract_payloads(res), vec![pl("11"), pl("22"), pl("33")]);
    let res = qh.take(&bar_q, &gamma, 3).await?;
    assert_eq!(extract_payloads(res), vec![pl("11"), pl("22"), pl("33"),]);

    let res = qh.take(&foo_q, &beta, 1).await?;
    assert_eq!(extract_payloads(res), vec![pl("x")]);

    let res: HashSet<_> = qh.queue_names().await?.into_iter().collect();
    assert_eq!(
        res,
        vec![bar_q.clone(), foo_q.clone()].into_iter().collect()
    );

    match qh.consumers(&foo_q).await? {
        GetConsumersResult::Consumers(consumers) => assert_eq!(
            consumers.into_iter().collect::<HashSet<_>>(),
            vec![alpha.clone(), beta.clone()].into_iter().collect()
        ),
        _ => panic!("no consumers"),
    };

    match qh.consumers(&bar_q).await? {
        GetConsumersResult::Consumers(consumers) => assert_eq!(
            consumers.into_iter().collect::<HashSet<_>>(),
            vec![alpha.clone(), gamma.clone()].into_iter().collect()
        ),
        _ => panic!("no consumers"),
    };

    let res = qh.stats(&qname("")).await?;
    let expected: HashMap<_, _> = vec![
        (
            foo_q.clone(),
            QueueStats {
                size: 3,
                consumers: 2,
                min_unconsumed_size: 1,
                max_unconsumed_size: 2,
            },
        ),
        (
            bar_q.clone(),
            QueueStats {
                size: 3,
                consumers: 2,
                min_unconsumed_size: 0,
                max_unconsumed_size: 3,
            },
        ),
    ]
    .into_iter()
    .collect();
    assert_eq!(expected, res);

    qh.collect_garbage().await?;
    let res = qh.stats(&qname("")).await?;
    let expected: HashMap<_, _> = vec![
        (
            foo_q.clone(),
            QueueStats {
                size: 2,
                consumers: 2,
                min_unconsumed_size: 1,
                max_unconsumed_size: 2,
            },
        ),
        (
            bar_q.clone(),
            QueueStats {
                size: 3,
                consumers: 2,
                min_unconsumed_size: 0,
                max_unconsumed_size: 3,
            },
        ),
    ]
    .into_iter()
    .collect();
    assert_eq!(expected, res);

    let res = qh.remove_consumer(&bar_q, &alpha).await?;
    assert_eq!(res, RemoveConsumerResult::Done);
    qh.collect_garbage().await?;
    let res = qh.stats(&qname("")).await?;
    let expected: HashMap<_, _> = vec![
        (
            foo_q.clone(),
            QueueStats {
                size: 2,
                consumers: 2,
                min_unconsumed_size: 1,
                max_unconsumed_size: 2,
            },
        ),
        (
            bar_q.clone(),
            QueueStats {
                size: 0,
                consumers: 1,
                min_unconsumed_size: 0,
                max_unconsumed_size: 0,
            },
        ),
    ]
    .into_iter()
    .collect();
    assert_eq!(expected, res);

    let res = qh.delete_queue(&bar_q).await?;
    assert_eq!(res, DeleteQueueResult::Done);
    qh.collect_garbage().await?;
    let res = qh.stats(&qname("")).await?;
    let expected: HashMap<_, _> = vec![(
        foo_q.clone(),
        QueueStats {
            size: 2,
            consumers: 2,
            min_unconsumed_size: 1,
            max_unconsumed_size: 2,
        },
    )]
    .into_iter()
    .collect();
    assert_eq!(expected, res);

    Ok(())
}

#[rstest]
#[case::in_memory(in_memory_qh())]
#[case::sqlite(sqlite_qh())]
async fn call_results<QH: QueueHub>(#[case] qh: QH) -> Result<()> {
    let qn = qname("foo");
    let c = cons("bar");

    let res = qh.delete_queue(&qn).await?;
    assert_eq!(res, DeleteQueueResult::QueueDoesNotExist);
    let res = qh.add_consumer(&qn, c.clone()).await?;
    assert_eq!(res, AddConsumerResult::QueueDoesNotExist);
    let res = qh.push(&qn, &[]).await?;
    if let PushMessagesResult::QueueDoesNotExist = res {
    } else {
        panic!("must be 'queue does not exist'")
    }
    let res = qh.read(&qn, &c, 3).await?;
    if let ReadMessagesResult::QueueDoesNotExist = res {
    } else {
        panic!("must be 'queue does not exist'")
    }
    let res = qh.take(&qn, &c, 3).await?;
    if let ReadMessagesResult::QueueDoesNotExist = res {
    } else {
        panic!("must be 'queue does not exist'")
    }

    let res = qh.create_queue(qn.clone()).await?;
    assert_eq!(res, CreateQueueResult::Done);
    let res = qh.create_queue(qn.clone()).await?;
    assert_eq!(res, CreateQueueResult::QueueAlreadyExists);

    let res = qh.remove_consumer(&qn, &c).await?;
    assert_eq!(res, RemoveConsumerResult::UnknownConsumer);
    let res = qh.read(&qn, &c, 3).await?;
    if let ReadMessagesResult::UnknownConsumer = res {
    } else {
        panic!("must be 'unknown consumer'")
    }
    let res = qh.take(&qn, &c, 3).await?;
    if let ReadMessagesResult::UnknownConsumer = res {
    } else {
        panic!("must be 'unknown consumer'")
    }
    let res = qh.add_consumer(&qn, c.clone()).await?;
    assert_eq!(res, AddConsumerResult::Done);
    let res = qh.add_consumer(&qn, c.clone()).await?;
    assert_eq!(res, AddConsumerResult::ConsumerAlreadyAdded);

    Ok(())
}
