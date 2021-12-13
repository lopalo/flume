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
    collections::{hash_map::Entry, BTreeMap, HashMap},
    convert::TryInto,
};

use super::*;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct SegmentIdx(u64);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct MessageIdx(u64);

//TODO: compact serialized representation: convertion to touple or single letter fieldnames
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Location {
    segment_idx: SegmentIdx,
    message_idx: MessageIdx,
}

impl Location {
    fn initial() -> Self {
        Self {
            segment_idx: SegmentIdx(0),
            message_idx: MessageIdx(0),
        }
    }
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
    message_quantity: usize,
    index_file: File,
    data_file: File,
}

type ReadSegments = BTreeMap<SegmentIdx, Mutex<ReadSegment>>;

struct Queue {
    directory: PathBuf,
    consumers: RwLock<HashMap<Consumer, RwLock<Location>>>,
    write_segment: Mutex<WriteSegment>,
    read_segments: RwLock<ReadSegments>,
}

type Queues = RwLock<HashMap<QueueName, Queue>>;

#[derive(Clone)]
pub struct AofQueueHub {
    directory: Arc<PathBuf>,
    bytes_per_segment: u64,
    queues: Arc<Queues>,
}

const INDEX_ITEM_SIZE: u64 = std::mem::size_of::<u64>() as u64;

impl AofQueueHub {
    pub async fn load(
        directory: PathBuf,
        bytes_per_segment: u64,
    ) -> Result<Self> {
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
            bytes_per_segment,
            queues: Arc::new(RwLock::new(queues)),
        })
    }

    async fn load_queue(queue_dir: &Path) -> Result<Queue> {
        let mut read_segments = BTreeMap::new();
        let mut next_seg_idx = SegmentIdx(0);
        let mut dir_entries = fs::read_dir(queue_dir).await?;
        while let Some(dir_entry) = dir_entries.next().await {
            let dir_entry = dir_entry?;
            let file_name =
                dir_entry.file_name().into_string().expect("Must be UTF-8");
            let segment_idx = match file_name.split_once('.') {
                Some((idx_str, "idx")) => {
                    let number = u64::from_str_radix(idx_str, 10)
                        .expect("Must be a number");
                    SegmentIdx(number)
                }
                _ => continue,
            };
            read_segments.insert(
                segment_idx.clone(),
                Mutex::new(
                    Self::load_read_segment(queue_dir, &segment_idx).await?,
                ),
            );
            if segment_idx > next_seg_idx {
                next_seg_idx = segment_idx;
            }
        }

        if !read_segments.is_empty() {
            next_seg_idx.0 += 1;
        }
        let write_segment = Mutex::new(
            Self::create_write_segment(queue_dir, &next_seg_idx).await?,
        );
        read_segments.insert(
            next_seg_idx.clone(),
            Mutex::new(
                Self::load_read_segment(queue_dir, &next_seg_idx).await?,
            ),
        );
        Ok(Queue {
            directory: queue_dir.to_owned(),
            write_segment,
            read_segments: RwLock::new(read_segments),
            consumers: RwLock::new(HashMap::new()),
        })
    }

    async fn check_segment_size(&self, queue: &Queue) -> Result<()> {
        let mut write_segment = queue.write_segment.lock().await;
        if write_segment.data_offset < self.bytes_per_segment {
            return Ok(());
        }
        let mut idx = write_segment.idx.clone();
        idx.0 += 1;
        *write_segment =
            Self::create_write_segment(&queue.directory, &idx).await?;

        queue.read_segments.write().await.insert(
            idx.clone(),
            Mutex::new(Self::load_read_segment(&queue.directory, &idx).await?),
        );
        Ok(())
    }

    async fn load_read_segment(
        queue_dir: &Path,
        idx: &SegmentIdx,
    ) -> Result<ReadSegment> {
        let mut open_opts = OpenOptions::new();
        open_opts.create(false).write(false).read(true);
        let (index_file, data_file) =
            Self::index_data_files(queue_dir, idx, open_opts).await?;
        let message_quantity =
            (index_file.metadata().await?.len() / INDEX_ITEM_SIZE) as usize;
        Ok(ReadSegment {
            message_quantity,
            index_file,
            data_file,
        })
    }

    async fn create_write_segment(
        queue_dir: &Path,
        idx: &SegmentIdx,
    ) -> Result<WriteSegment> {
        let mut open_opts = OpenOptions::new();
        open_opts.create(true).append(true).read(false);
        let (index_file, data_file) =
            Self::index_data_files(queue_dir, idx, open_opts).await?;
        Ok(WriteSegment {
            idx: idx.clone(),
            next_message_idx: MessageIdx(0),
            data_offset: 0,
            index_file,
            data_file,
        })
    }

    async fn index_data_files(
        queue_dir: &Path,
        idx: &SegmentIdx,
        options: OpenOptions,
    ) -> Result<(File, File)> {
        let mut index_path = queue_dir.to_owned();
        let mut data_path = queue_dir.to_owned();
        index_path.push(format!("{:020}.idx", idx.0));
        data_path.push(format!("{:020}.data", idx.0));
        let index_file = options.open(index_path).await?;
        let data_file = options.open(data_path).await?;
        Ok((index_file, data_file))
    }

    async fn read_messages(
        segments: &ReadSegments,
        start_location: &Location,
        mut number: usize,
    ) -> Result<Messages<Self>> {
        let segments = segments.range(&start_location.segment_idx..);
        let segment_beginning = MessageIdx(0);
        let mut start_msg_idx = &start_location.message_idx;
        let mut batch = Vec::new();
        for (segment_idx, segment) in segments {
            if number == 0 {
                break;
            };
            if *segment_idx != start_location.segment_idx {
                start_msg_idx = &segment_beginning
            }
            let mut segment = segment.lock().await;
            let messages = Self::read_messages_from_segment(
                segment_idx,
                &mut segment,
                start_msg_idx,
                number,
            )
            .await?;
            number -= messages.len();
            batch.extend(messages);
        }
        Ok(batch)
    }

    async fn read_messages_from_segment(
        segment_idx: &SegmentIdx,
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

        let data_file = &mut segment.data_file;
        let mut data_offset = offsets.next().unwrap_or(0);
        data_file.seek(SeekFrom::Start(data_offset)).await?;
        let mut data_reader = BufReader::new(data_file);
        let mut batch = Vec::with_capacity(number);
        let mut message_idx = start_message_idx.clone();
        for payload_offset in offsets {
            let len = (payload_offset - data_offset) as usize;
            data_offset = payload_offset;
            let mut buf = vec![0; len];
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
            match q.consumers.write().await.entry(consumer) {
                Entry::Occupied(_) => ConsumerAlreadyAdded,
                Entry::Vacant(entry) => {
                    entry.insert(RwLock::new(Location::initial()));
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
                self.check_segment_size(q).await?;

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
                q.read_segments
                    .read()
                    .await
                    .get(&segment_idx)
                    .unwrap()
                    .lock()
                    .await
                    .message_quantity = next_msg_idx.0 as usize;
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
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(location) => {
                        let loc = location.read().await;
                        let read_segments = q.read_segments.read().await;
                        let batch =
                            Self::read_messages(&read_segments, &loc, number)
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
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(pos_lock) => {
                        let mut consumer_pos = pos_lock.write().await;
                        if *position < *consumer_pos {
                            return Ok(PositionIsOutOfQueue);
                        }
                        let read_segments = q.read_segments.read().await;
                        let message_quantity =
                            match read_segments.get(&position.segment_idx) {
                                Some(s) => s.lock().await.message_quantity,
                                None => return Ok(PositionIsOutOfQueue),
                            } as u64;
                        if position.message_idx.0 >= message_quantity {
                            return Ok(PositionIsOutOfQueue);
                        }
                        let prev_pos = consumer_pos.clone();
                        *consumer_pos = position.clone();
                        consumer_pos.message_idx.0 += 1;
                        //TODO: calculate correct value from multiple segments
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
                let consumers = q.consumers.read().await;
                match consumers.get(consumer) {
                    Some(location) => {
                        let mut loc = location.write().await;
                        let read_segments = q.read_segments.read().await;
                        let batch =
                            Self::read_messages(&read_segments, &loc, number)
                                .await?;
                        loc.message_idx.0 += batch.len() as u64;
                        if let Some(last_msg) = batch.last() {
                            *loc = last_msg.position.clone();
                            loc.message_idx.0 += 1;
                        }
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
