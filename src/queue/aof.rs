use async_std::{
    fs::{self, File, OpenOptions},
    io::{
        prelude::SeekExt, BufReader, BufWriter, Error, ReadExt, Result,
        SeekFrom, WriteExt,
    },
    path::{Path, PathBuf},
    stream::StreamExt,
    sync::{Arc, Mutex, RwLock},
};
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    convert::TryInto,
};

use super::*;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct SegmentIdx(u64);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct MessageIdx(u64);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Location {
    segment_idx: SegmentIdx,
    message_idx: MessageIdx,
}

struct WriteSegment {
    idx: SegmentIdx,
    next_message_idx: MessageIdx,
    data_offset: u64,
    index_file: File,
    data_file: File,
}

//TODO: a pool of open files for each read segment
struct ReadSegment {
    idx: SegmentIdx,
    message_quantity: usize,
    index_file: File,
    data_file: File,
}

struct Queue {
    read_segments: VecDeque<Mutex<ReadSegment>>,
    write_segment: Mutex<WriteSegment>,
    consumers: RwLock<HashMap<Consumer, RwLock<Location>>>,
}

type Queues = RwLock<HashMap<QueueName, Queue>>;

#[derive(Clone)]
pub struct AofQueueHub {
    directory: Arc<PathBuf>,
    queues: Arc<Queues>,
}

const INDEX_ITEM_SIZE: u64 = std::mem::size_of::<u64>() as u64;

impl AofQueueHub {
    pub async fn load(directory: PathBuf) -> Result<Self> {
        //TODO: directory locking
        //TODO: load queues in parallel

        let mut queues = HashMap::new();
        let mut dir_entries = fs::read_dir(&directory).await?;
        while let Some(dir_entry) = dir_entries.next().await {
            let dir_entry = dir_entry?;
            let file_name =
                dir_entry.file_name().into_string().expect("Must be UTF-8");
            let queue = Self::load_queue(dir_entry.path().as_ref()).await?;
            queues.insert(QueueName::new(file_name), queue);
        }
        Ok(Self {
            directory: Arc::new(directory),
            queues: Arc::new(RwLock::new(queues)),
        })
    }

    async fn load_queue(queue_dir: &Path) -> Result<Queue> {
        let mut read_segments = VecDeque::new();
        //TODO: load multiple read segments
        //TODO: derive the new read and write segments' index from the last read_segment
        let idx = SegmentIdx(0);
        let write_segment =
            Mutex::new(Self::load_write_segment(queue_dir, idx.clone()).await?);
        read_segments.push_back(Mutex::new(
            Self::load_read_segment(queue_dir, idx).await?,
        ));
        Ok(Queue {
            read_segments,
            write_segment,
            consumers: RwLock::new(HashMap::new()),
        })
    }

    async fn load_read_segment(
        queue_dir: &Path,
        idx: SegmentIdx,
    ) -> Result<ReadSegment> {
        let mut open_opts = OpenOptions::new();
        open_opts.create(false).write(false).read(true);
        let (index_file, data_file) =
            Self::index_data_files(queue_dir, idx, open_opts).await?;
        let message_quantity =
            (index_file.metadata().await?.len() / INDEX_ITEM_SIZE) as usize;
        Ok(ReadSegment {
            idx: SegmentIdx(0),
            message_quantity,
            index_file,
            data_file,
        })
    }

    async fn load_write_segment(
        queue_dir: &Path,
        idx: SegmentIdx,
    ) -> Result<WriteSegment> {
        let mut open_opts = OpenOptions::new();
        open_opts.create(true).append(true).read(false);
        let (index_file, data_file) =
            Self::index_data_files(queue_dir, idx, open_opts).await?;
        let next_message_idx =
            MessageIdx(index_file.metadata().await?.len() / INDEX_ITEM_SIZE);
        let data_offset = data_file.metadata().await?.len();
        Ok(WriteSegment {
            idx: SegmentIdx(0),
            next_message_idx,
            data_offset,
            index_file,
            data_file,
        })
    }

    async fn index_data_files(
        queue_dir: &Path,
        idx: SegmentIdx,
        options: OpenOptions,
    ) -> Result<(File, File)> {
        let mut index_path = queue_dir.to_owned();
        let mut data_path = queue_dir.to_owned();
        index_path.push(format!("{}.idx", idx.0));
        data_path.push(format!("{}.data", idx.0));
        let index_file = options.open(index_path).await?;
        let data_file = options.open(data_path).await?;
        Ok((index_file, data_file))
    }

    async fn read_messages_from_segment(
        segment: &mut ReadSegment,
        start_message_idx: &MessageIdx,
        mut number: usize,
    ) -> Result<Messages<Self>> {
        let is_segment_beginning = start_message_idx.0 == 0;
        let mut start_idx_offset = start_message_idx.0 * INDEX_ITEM_SIZE;
        number = number.min(segment.message_quantity);
        let idx_item_size = INDEX_ITEM_SIZE as usize;
        //the last offset before the requested range is needed to determine
        //the offset and length of the first message in the range
        let len = (number + 1) * idx_item_size;
        let mut index_bytes = vec![0; len];
        let index_buf = if is_segment_beginning {
            &mut index_bytes[idx_item_size..]
        } else {
            start_idx_offset -= INDEX_ITEM_SIZE;
            &mut index_bytes[..]
        };
        let index_file = &mut segment.index_file;
        index_file.seek(SeekFrom::Start(start_idx_offset)).await?;
        let mut read_bytes = index_file.read(index_buf).await?;
        if is_segment_beginning {
            read_bytes += idx_item_size;
        };
        let mut offsets = index_bytes[..read_bytes]
            .chunks_exact(idx_item_size)
            .map(|bs| u64::from_be_bytes(bs.try_into().unwrap()));

        let segment_idx = segment.idx.clone();
        let data_file = &mut segment.data_file;
        let mut data_offset = offsets.next().unwrap_or(0);
        data_file.seek(SeekFrom::Start(data_offset)).await?;
        let mut data_reader = BufReader::new(data_file);
        let mut batch = Vec::with_capacity(number);
        let mut message_idx = start_message_idx.clone();
        for payload_offset in offsets {
            let len = (payload_offset - data_offset) as usize;
            data_offset = payload_offset;
            let mut buf = vec![0u8; len];
            //TODO: break the loop on ErrorKind::UnexpectedEof?
            data_reader.read_exact(&mut buf[..]).await?;
            let payload = String::from_utf8(buf).unwrap();
            batch.push(Message {
                position: Location {
                    segment_idx: segment_idx.clone(),
                    message_idx: message_idx.clone(),
                },
                payload: Payload(payload),
            });
            message_idx.0 += 1;
        }
        Ok(batch)
    }
}

#[async_trait]
impl QueueHub for AofQueueHub {
    type Position = Location;
    type PayloadData = String;
    type Error = Error;

    fn payload(data: String) -> Payload<Self> {
        Payload::new(data)
    }

    async fn create_queue(
        &self,
        queue_name: QueueName,
    ) -> Result<CreateQueueResult> {
        use CreateQueueResult::*;

        let mut qs = self.queues.write().await;
        Ok(if qs.contains_key(&queue_name) {
            QueueAlreadyExists
        } else {
            let mut queue_dir = self.directory.as_ref().clone();
            queue_dir.push(&queue_name.0);
            fs::create_dir(&queue_dir).await?;
            let mut index_path = queue_dir.clone();
            let mut data_path = queue_dir.clone();
            index_path.push("0.idx");
            data_path.push("0.data");
            File::create(data_path).await?;
            File::create(index_path).await?;
            let queue = Self::load_queue(queue_dir.as_ref()).await?;
            qs.insert(queue_name, queue);
            Done
        })
    }

    async fn delete_queue(
        &self,
        queue_name: &QueueName,
    ) -> Result<DeleteQueueResult> {
        use DeleteQueueResult::*;

        let mut qs = self.queues.write().await;
        Ok(if qs.contains_key(&queue_name) {
            qs.remove(queue_name);
            let mut queue_dir = self.directory.as_ref().clone();
            queue_dir.push(&queue_name.0);
            fs::remove_dir_all(&queue_dir).await?;
            Done
        } else {
            QueueDoesNotExist
        })
    }

    async fn add_consumer(
        &self,
        queue_name: &QueueName,
        consumer: Consumer,
    ) -> Result<AddConsumerResult> {
        use AddConsumerResult::*;

        //TODO: make consumer's state persistent
        Ok(if let Some(q) = self.queues.read().await.get(queue_name) {
            let segment_idx =
                q.read_segments.front().unwrap().lock().await.idx.clone();
            let location = Location {
                segment_idx,
                message_idx: MessageIdx(0),
            };
            match q.consumers.write().await.entry(consumer) {
                Entry::Occupied(_) => ConsumerAlreadyAdded,
                Entry::Vacant(entry) => {
                    entry.insert(RwLock::new(location));
                    Done
                }
            }
        } else {
            QueueDoesNotExist
        })
    }

    async fn remove_consumer(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
    ) -> Result<RemoveConsumerResult> {
        unimplemented!()
    }

    async fn push(
        &self,
        queue_name: &QueueName,
        batch: &[Payload<Self>],
    ) -> Result<PushMessagesResult<Self>> {
        use PushMessagesResult::*;

        let qs = self.queues.read().await;
        Ok(match qs.get(queue_name) {
            Some(q) => {
                let mut locations = Vec::with_capacity(batch.len());
                let mut offsets = Vec::with_capacity(batch.len());

                let mut segment_guard = q.write_segment.lock().await;
                let segment = &mut *segment_guard;
                let segment_idx = segment.idx.clone();
                let mut next_msg_idx = segment.next_message_idx.clone();
                let mut data_offset = segment.data_offset;

                {
                    let mut data_file = &mut segment.data_file;
                    let mut data_writer = BufWriter::new(&mut data_file);
                    for payload in batch {
                        let bs = payload.0.as_bytes();
                        data_writer.write_all(bs).await?;
                        locations.push(Location {
                            segment_idx: segment_idx.clone(),
                            message_idx: next_msg_idx.clone(),
                        });
                        next_msg_idx.0 += 1;
                        data_offset += bs.len() as u64;
                        offsets.push(data_offset)
                    }
                    data_writer.flush().await?;
                    data_file.sync_all().await?;
                }

                let mut index_file = &mut segment.index_file;
                let mut index_writer = BufWriter::new(&mut index_file);
                for offset in offsets {
                    index_writer.write(&offset.to_be_bytes()).await?;
                }
                index_writer.flush().await?;
                index_file.sync_all().await?;

                segment.next_message_idx = next_msg_idx.clone();
                segment.data_offset = data_offset;
                drop(segment_guard);
                for segment in q.read_segments.iter().rev() {
                    let mut s = segment.lock().await;
                    if s.idx == segment_idx {
                        s.message_quantity = next_msg_idx.0 as usize;
                        break;
                    }
                }

                Done(locations)
            }
            None => QueueDoesNotExist,
        })
    }

    async fn read(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self>> {
        use ReadMessagesResult::*;

        let qs = self.queues.read().await;
        Ok(match qs.get(queue_name) {
            Some(q) => {
                let mut segment = q.read_segments.front().unwrap().lock().await;
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(location) => {
                        let loc = location.write().await;
                        let batch = Self::read_messages_from_segment(
                            &mut segment,
                            &loc.message_idx,
                            number,
                        )
                        .await?;
                        Messages(batch)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        })
    }

    async fn commit(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        position: &Self::Position,
    ) -> Result<CommitMessagesResult> {
        use CommitMessagesResult::*;

        let qs = self.queues.read().await;
        Ok(match qs.get(queue_name) {
            Some(q) => {
                let segment = q.read_segments.front().unwrap().lock().await;
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(pos_lock) => {
                        let mut pos_guard = pos_lock.write().await;
                        let consumer_pos = &mut *pos_guard;
                        if position < consumer_pos {
                            return Ok(PositionIsOutOfQueue);
                        }
                        if position.segment_idx != segment.idx {
                            return Ok(PositionIsOutOfQueue);
                        }
                        if position.message_idx.0
                            >= segment.message_quantity as u64
                        {
                            return Ok(PositionIsOutOfQueue);
                        }
                        let prev_pos = consumer_pos.clone();
                        *consumer_pos = position.clone();
                        consumer_pos.message_idx.0 += 1;
                        let committed =
                            consumer_pos.message_idx.0 - prev_pos.message_idx.0;
                        Committed(committed as usize)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        })
    }

    async fn take(
        &self,
        queue_name: &QueueName,
        consumer: &Consumer,
        number: usize,
    ) -> Result<ReadMessagesResult<Self>> {
        use ReadMessagesResult::*;

        let qs = self.queues.read().await;
        Ok(match qs.get(queue_name) {
            Some(q) => {
                let mut segment = q.read_segments.front().unwrap().lock().await;
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(location) => {
                        let mut loc = location.write().await;
                        let batch = Self::read_messages_from_segment(
                            &mut segment,
                            &loc.message_idx,
                            number,
                        )
                        .await?;
                        loc.message_idx.0 += batch.len() as u64;
                        Messages(batch)
                    }
                    None => UnknownConsumer,
                }
            }
            None => QueueDoesNotExist,
        })
    }

    async fn collect_garbage(&self) -> Result<()> {
        //TODO: there must be at least one read segment in each queue
        Ok(())
    }

    async fn queue_names(&self) -> Result<Vec<QueueName>> {
        //TODO:
        Ok(vec![])
    }

    async fn consumers(
        &self,
        queue_name: &QueueName,
    ) -> Result<GetConsumersResult> {
        unimplemented!()
    }

    async fn stats(&self, queue_name_prefix: &QueueName) -> Result<Stats> {
        unimplemented!()
    }
}
