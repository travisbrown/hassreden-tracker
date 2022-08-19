use super::{
    formats::archive::{write_batch, FollowReader},
    Batch, Change,
};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::RwLock;
use zstd::stream::{read::Decoder, write::Encoder};

const CURRENT_FILE_NAME: &str = "current.bin";
const PAST_DIR_NAME: &str = "past";
const RUN_DURATION_BUFFER_S: i64 = 20 * 60;
const ZSTD_LEVEL: i32 = 7;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Archive error")]
    Archive(#[from] super::formats::archive::Error),
    #[error("Duplicate user ID")]
    DuplicateId { target_id: u64, source_id: u64 },
    #[error("Missing user ID")]
    MissingId { target_id: u64, source_id: u64 },
    #[error("User ID is not tracked")]
    UntrackedId(u64),
    #[error("Batch is stale")]
    StaleBatch(Batch),
    #[error("Past file exists")]
    PastFileCollision(Box<Path>),
    #[error("Invalid past file path")]
    InvalidPastFile(Box<Path>),
    #[error("Invalid batch")]
    InvalidBatch(Batch),
}

struct UserState {
    followers: HashSet<u64>,
    following: HashSet<u64>,
    last_update: DateTime<Utc>,
    expiration: Option<DateTime<Utc>>,
}

impl UserState {
    fn new(last_update: DateTime<Utc>) -> Self {
        Self {
            followers: HashSet::new(),
            following: HashSet::new(),
            last_update,
            expiration: None,
        }
    }
}

struct State {
    users: HashMap<u64, UserState>,
    writer: BufWriter<File>,
}

impl State {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let writer = BufWriter::new(OpenOptions::new().append(true).create(true).open(path)?);

        Ok(Self {
            users: HashMap::new(),
            writer,
        })
    }

    fn make_batch(
        &self,
        user_id: u64,
        follower_ids: HashSet<u64>,
        following_ids: HashSet<u64>,
    ) -> Batch {
        match self.users.get(&user_id) {
            None => {
                let mut follower_ids = follower_ids.into_iter().collect::<Vec<_>>();
                follower_ids.sort_unstable();

                let mut following_ids = following_ids.into_iter().collect::<Vec<_>>();
                following_ids.sort_unstable();

                Batch::new(
                    Utc::now(),
                    user_id,
                    Some(Change::new(follower_ids, vec![])),
                    Some(Change::new(following_ids, vec![])),
                )
            }
            Some(user_state) => {
                todo!()
            }
        }
    }

    fn update_and_write(&mut self, batch: &Batch, last_update: DateTime<Utc>) -> Result<(), Error> {
        self.update(batch, Some(last_update))?;
        self.write(batch)?;
        Ok(())
    }

    fn update(&mut self, batch: &Batch, last_update: Option<DateTime<Utc>>) -> Result<(), Error> {
        let user_state = self
            .users
            .entry(batch.user_id)
            .or_insert_with(|| UserState::new(batch.timestamp));

        if last_update
            .map(|last_update| last_update != user_state.last_update)
            .unwrap_or(false)
        {
            Err(Error::StaleBatch(batch.clone()))
        } else {
            user_state.last_update = batch.timestamp;
            user_state.expiration = None;

            if let Some(change) = &batch.follower_change {
                for id in &change.addition_ids {
                    if !user_state.followers.insert(*id) {
                        return Err(Error::DuplicateId {
                            target_id: batch.user_id,
                            source_id: *id,
                        });
                    }
                }

                for id in &change.removal_ids {
                    if !user_state.followers.remove(id) {
                        return Err(Error::MissingId {
                            target_id: batch.user_id,
                            source_id: *id,
                        });
                    }
                }
            }

            if let Some(change) = &batch.followed_change {
                for id in &change.addition_ids {
                    if !user_state.following.insert(*id) {
                        return Err(Error::DuplicateId {
                            target_id: batch.user_id,
                            source_id: *id,
                        });
                    }
                }

                for id in &change.removal_ids {
                    if !user_state.following.remove(id) {
                        return Err(Error::MissingId {
                            target_id: batch.user_id,
                            source_id: *id,
                        });
                    }
                }
            }

            Ok(())
        }
    }

    fn write(&mut self, batch: &Batch) -> Result<(), Error> {
        Ok(write_batch(&mut self.writer, batch)?)
    }
}

pub struct Store {
    base: Box<Path>,
    state: RwLock<State>,
}

impl Store {
    pub fn load<P: AsRef<Path>>(base: P) -> Result<Self, Error> {
        let store = Self {
            base: base.as_ref().to_path_buf().into_boxed_path(),
            state: RwLock::new(State::new(base.as_ref().join(CURRENT_FILE_NAME))?),
        };

        let current_batches = store.current_batches()?;

        let mut state = store.state.write().unwrap();

        for result in store.past_batches() {
            let (_, batch) = result?;
            state.update(&batch, None)?;
        }

        for batch in current_batches {
            state.update(&batch, None)?;
        }

        std::mem::drop(state);

        Ok(store)
    }

    pub fn user_count(&self) -> usize {
        self.state.read().unwrap().users.len()
    }

    pub fn followers(&self) -> Vec<(u64, Vec<u64>)> {
        let state = self.state.read().unwrap();
        let mut results = state
            .users
            .iter()
            .map(|(id, user_state)| {
                let mut results = user_state.followers.iter().copied().collect::<Vec<_>>();
                results.sort_unstable();
                (*id, results)
            })
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    pub fn following(&self) -> Vec<(u64, Vec<u64>)> {
        let state = self.state.read().unwrap();
        let mut results = state
            .users
            .iter()
            .map(|(id, user_state)| {
                let mut results = user_state.following.iter().copied().collect::<Vec<_>>();
                results.sort_unstable();
                (*id, results)
            })
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    fn past_dir_path(&self) -> Box<Path> {
        self.base.join(PAST_DIR_NAME).into_boxed_path()
    }

    fn current_file_path(&self) -> Box<Path> {
        self.base.join(CURRENT_FILE_NAME).into_boxed_path()
    }

    fn past_batches(&self) -> PastBatchIterator<ZstFollowReader<'_>> {
        std::fs::read_dir(self.past_dir_path())
            .map_err(Error::from)
            .and_then(|entries| {
                let mut paths = entries
                    .map(|result| {
                        result.map_err(Error::from).and_then(|entry| {
                            extract_path_date(entry.path())
                                .map(|date| (date, entry.path().into_boxed_path()))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                paths.sort();
                paths.reverse();

                Ok(paths)
            })
            .map_or_else(
                |error| PastBatchIterator::Failed(Some(Error::from(error))),
                |paths| PastBatchIterator::Remaining {
                    paths,
                    current: None,
                },
            )
    }

    fn past_date_path(&self, date: NaiveDate) -> Box<Path> {
        self.base
            .join(PAST_DIR_NAME)
            .join(format!("{}.bin.zst", date.format("%Y-%m-%d")))
            .into_boxed_path()
    }

    pub fn current_batches(&self) -> Result<Vec<Batch>, Error> {
        let _guard = self.state.read().unwrap();
        let file = File::open(self.current_file_path())?;

        let mut batches = FollowReader::new(BufReader::new(file))
            .map(|result| result.map_err(Error::from))
            .collect::<Result<Vec<_>, _>>()?;

        batches.sort();

        Ok(batches)
    }

    /// Moves all batches from previous days in the current workspace into the past directory.
    pub fn archive(&self) -> Result<usize, Error> {
        let mut state = self.state.write().unwrap();
        state.writer.flush()?;

        let file = File::open(self.current_file_path())?;
        let mut by_date = HashMap::new();

        for result in FollowReader::new(BufReader::new(file)) {
            let batch = result?;
            by_date
                .entry(batch.timestamp.date_naive())
                .or_insert_with(Vec::new)
                .push(batch);
        }

        let mut batches = by_date.into_iter().collect::<Vec<_>>();
        batches.sort();

        let current_date = Utc::now().date_naive();
        let to_archive = batches
            .iter()
            .filter(|(date, _)| *date != current_date)
            .collect::<Vec<_>>();

        if !to_archive.is_empty() {
            let by_path = to_archive
                .iter()
                .map(|(date, batches)| (self.past_date_path(*date), batches))
                .collect::<Vec<_>>();

            if let Some((path, _)) = by_path.iter().find(|(path, _)| path.exists()) {
                Err(Error::PastFileCollision(path.clone()))
            } else {
                let mut archived_count = 0;

                for (path, batches) in by_path {
                    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
                    let mut writer = Encoder::new(file, ZSTD_LEVEL)?.auto_finish();

                    for batch in batches {
                        archived_count += 1;
                        write_batch(&mut writer, batch)?;
                    }
                }

                let file = File::create(self.current_file_path())?;
                state.writer = BufWriter::new(file);

                if let Some((_, batches)) = batches.iter().find(|(date, _)| *date == current_date) {
                    for batch in batches {
                        write_batch(&mut state.writer, batch)?;
                    }
                }

                Ok(archived_count)
            }
        } else {
            Ok(0)
        }
    }

    /// Declares an intention to scrape this account, reserving it for an amount of time estimated from the given approximate follower count.
    ///
    /// Result will be empty if the account is already reserved.
    pub fn check_out(
        &self,
        user_id: u64,
        count_estimate: usize,
    ) -> Result<Option<DateTime<Utc>>, Error> {
        let now = Utc::now();

        let mut state = self.state.write().unwrap();
        let user_state = state
            .users
            .get_mut(&user_id)
            .ok_or(Error::UntrackedId(user_id))?;

        if user_state
            .expiration
            .filter(|&expiration| expiration > now)
            .is_none()
        {
            user_state.expiration = Some(now + estimate_run_duration(count_estimate));
            Ok(Some(user_state.last_update))
        } else {
            Ok(None)
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        let mut last_timestamp = DateTime::<Utc>::MIN_UTC;
        let mut last_user_id = 0;

        self.state.write().unwrap().writer.flush()?;

        for result in self.past_batches() {
            let (date, batch) = result?;

            if date != batch.timestamp.date_naive() || last_timestamp > batch.timestamp {
                return Err(Error::InvalidBatch(batch.clone()));
            }

            if last_timestamp == batch.timestamp && last_user_id >= batch.user_id {
                return Err(Error::InvalidBatch(batch.clone()));
            }

            last_timestamp = batch.timestamp;
            last_user_id = batch.user_id;
        }

        let now_date = Utc::now().date_naive();

        for batch in self.current_batches()? {
            if batch.timestamp.date_naive() != now_date {
                return Err(Error::InvalidBatch(batch.clone()));
            }
        }

        Ok(())
    }

    //·pub ages(&self) -> Vec<
}

type ZstFollowReader<'a> = FollowReader<BufReader<Decoder<'a, BufReader<File>>>>;

pub enum PastBatchIterator<R> {
    Failed(Option<Error>),
    Remaining {
        paths: Vec<(NaiveDate, Box<Path>)>,
        current: Option<(NaiveDate, R)>,
    },
}

impl Iterator for PastBatchIterator<ZstFollowReader<'_>> {
    type Item = Result<(NaiveDate, Batch), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Failed(error) => error.take().map(Err),
            Self::Remaining {
                ref mut paths,
                ref mut current,
            } => match current {
                Some((date, reader)) => match reader.next() {
                    Some(result) => Some(result.map_err(Error::from).map(|batch| (*date, batch))),
                    None => {
                        *current = None;
                        self.next()
                    }
                },
                None => match paths.pop() {
                    None => None,
                    Some((date, path)) => match File::open(path).and_then(Decoder::new) {
                        Err(error) => Some(Err(Error::from(error))),
                        Ok(decoder) => {
                            *current = Some((date, FollowReader::new(BufReader::new(decoder))));
                            self.next()
                        }
                    },
                },
            },
        }
    }
}

fn extract_path_date<P: AsRef<Path>>(path: P) -> Result<NaiveDate, Error> {
    let date_str = path
        .as_ref()
        .file_name()
        .and_then(|ostr| ostr.to_str())
        .and_then(|str| str.split('.').next())
        .ok_or_else(|| Error::InvalidPastFile(path.as_ref().to_path_buf().into_boxed_path()))?;

    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|_| Error::InvalidPastFile(path.as_ref().to_path_buf().into_boxed_path()))
}

fn estimate_run_duration(count: usize) -> Duration {
    Duration::seconds((((count / 75000) + 1) * 15 * 60) as i64)
        + Duration::seconds(RUN_DURATION_BUFFER_S)
}
