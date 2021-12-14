use async_std::{
    fs::{self, File, OpenOptions},
    io::{
        prelude::SeekExt, BufReader, BufWriter, Error, ErrorKind, ReadExt,
        Result, SeekFrom, WriteExt,
    },
    path::{Path, PathBuf},
    stream::StreamExt,
    sync::{Arc, Mutex, RwLock},
};
use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    convert::{TryFrom, TryInto},
    fmt,
    result::Result as StdResult,
};

use super::*;

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct SegmentIdx(u64);

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct MessageIdx(u64);

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "&str")]
#[serde(into = "String")]
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

impl From<Location> for String {
    fn from(loc: Location) -> String {
        format!("{}.{}", loc.segment_idx.0, loc.message_idx.0)
    }
}

pub struct InvalidLocationString;

impl fmt::Display for InvalidLocationString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid location string")
    }
}

impl TryFrom<&str> for Location {
    type Error = InvalidLocationString;

    fn try_from(s: &str) -> StdResult<Location, Self::Error> {
        match s.split_once('.') {
            Some((seg_idx_str, msg_idx_str)) => {
                let segment_idx = u64::from_str_radix(seg_idx_str, 10)
                    .map(SegmentIdx)
                    .or(Err(InvalidLocationString))?;
                let message_idx = u64::from_str_radix(msg_idx_str, 10)
                    .map(MessageIdx)
                    .or(Err(InvalidLocationString))?;
                Ok(Self {
                    segment_idx,
                    message_idx,
                })
            }
            None => Err(InvalidLocationString),
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
            consumers: RwLock::new(HashMap::new()),
            write_segment,
            read_segments: RwLock::new(read_segments),
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
        let (index_path, data_path) = Self::index_data_paths(queue_dir, idx);
        let index_file = options.open(index_path).await?;
        let data_file = options.open(data_path).await?;
        Ok((index_file, data_file))
    }

    fn index_data_paths(
        queue_dir: &Path,
        idx: &SegmentIdx,
    ) -> (PathBuf, PathBuf) {
        let mut index_path = queue_dir.to_owned();
        let mut data_path = queue_dir.to_owned();
        index_path.push(format!("{:020}.idx", idx.0));
        data_path.push(format!("{:020}.data", idx.0));
        (index_path, data_path)
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
            match data_reader.read_exact(&mut buf[..]).await {
                Ok(..) => (),
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                res => res?,
            };
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

    async fn distance(
        read_segments: &ReadSegments,
        from: &Location,
        to: &Location,
    ) -> usize {
        let segments = read_segments.range(&from.segment_idx..=&to.segment_idx);
        let mut distance = 0;
        if from.segment_idx == to.segment_idx {
            return (to.message_idx.0 - from.message_idx.0) as usize;
        }
        for (segment_idx, segment) in segments {
            let msg_quantity = segment.lock().await.message_quantity;
            if segment_idx == &from.segment_idx {
                let msg_quantity = segment.lock().await.message_quantity;
                let from_msg_quantity = from.message_idx.0 as usize;
                distance += msg_quantity - from_msg_quantity;
            } else if segment_idx == &to.segment_idx {
                let to_msg_quantity = to.message_idx.0 as usize;
                distance += to_msg_quantity;
            } else {
                let msg_quantity = segment.lock().await.message_quantity;
                distance += msg_quantity;
            };
        }
        distance
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
        use RemoveConsumerResult::*;

        Ok(if let Some(q) = self.queues.read().await.get(queue_name) {
            match q.consumers.write().await.remove(consumer) {
                Some(_) => Done,
                None => UnknownConsumer,
            }
        } else {
            QueueDoesNotExist
        })
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
                        let committed = Self::distance(
                            &read_segments,
                            &prev_pos,
                            &consumer_pos,
                        )
                        .await;
                        Committed(committed)
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
        for queue in self.queues.read().await.values() {
            let consumers = queue.consumers.read().await;
            if consumers.is_empty() {
                continue;
            };
            let mut min_sig_idx = SegmentIdx(u64::MAX);

            for consumer_pos in consumers.values() {
                min_sig_idx = min_sig_idx
                    .min(consumer_pos.read().await.segment_idx.clone());
            }
            //TODO: save consumers here
            let write_segment = queue.write_segment.lock().await;
            let read_segments = &mut queue.read_segments.write().await;
            let garbage: Vec<_> = read_segments
                .range(..min_sig_idx)
                .map(|(seg_idx, _)| seg_idx.clone())
                .filter(|seg_idx| seg_idx != &write_segment.idx)
                .collect();
            for seg_idx in garbage {
                read_segments.remove(&seg_idx).unwrap();
                let (index_path, data_path) =
                    Self::index_data_paths(&queue.directory, &seg_idx);
                fs::remove_file(index_path).await?;
                fs::remove_file(data_path).await?;
            }
        }
        Ok(())
    }

    async fn queue_names(&self) -> Result<Vec<QueueName>> {
        Ok(self.queues.read().await.keys().map(Clone::clone).collect())
    }

    async fn consumers(
        &self,
        queue_name: &QueueName,
    ) -> Result<GetConsumersResult> {
        use GetConsumersResult::*;

        Ok(if let Some(q) = self.queues.read().await.get(queue_name) {
            Consumers(
                q.consumers.read().await.keys().map(|c| c.clone()).collect(),
            )
        } else {
            QueueDoesNotExist
        })
    }

    async fn stats(&self, queue_name_prefix: &QueueName) -> Result<Stats> {
        let mut res = HashMap::new();
        let qs = self.queues.read().await;
        for (queue_name, queue) in qs.iter() {
            if !queue_name.0.starts_with(&queue_name_prefix.0) {
                continue;
            }
            let consumers = queue.consumers.read().await;
            let mut consumer_locations = vec![];
            for consumer_loc in consumers.values() {
                consumer_locations.push(consumer_loc.read().await.clone())
            }
            let read_segments = queue.read_segments.read().await;
            let (last_seg_idx, last_seg) =
                read_segments.iter().rev().next().unwrap();
            let last_loc = Location {
                segment_idx: last_seg_idx.clone(),
                message_idx: MessageIdx(
                    last_seg.lock().await.message_quantity as u64,
                ),
            };
            let (first_seg_idx, _) = read_segments.iter().next().unwrap();
            let first_loc = Location {
                segment_idx: first_seg_idx.clone(),
                message_idx: MessageIdx(0),
            };
            let mut min_consumer_loc = last_loc.clone();
            let mut max_consumer_loc = first_loc.clone();
            for mut consumer_loc in consumer_locations {
                consumer_loc = consumer_loc.max(first_loc.clone());
                min_consumer_loc = min_consumer_loc.min(consumer_loc.clone());
                max_consumer_loc = max_consumer_loc.max(consumer_loc);
            }

            let mut first_loc = first_loc;
            if first_loc.segment_idx == min_consumer_loc.segment_idx {
                first_loc.message_idx = min_consumer_loc.message_idx.clone();
            }
            let size =
                Self::distance(&read_segments, &first_loc, &last_loc).await;
            let min_unconsumed_size =
                Self::distance(&read_segments, &max_consumer_loc, &last_loc)
                    .await;

            let max_unconsumed_size =
                Self::distance(&read_segments, &min_consumer_loc, &last_loc)
                    .await;
            let q_stats = QueueStats {
                size,
                consumers: consumers.len(),
                min_unconsumed_size,
                max_unconsumed_size,
            };
            res.insert(queue_name.clone(), q_stats);
        }
        Ok(res)
    }
}
