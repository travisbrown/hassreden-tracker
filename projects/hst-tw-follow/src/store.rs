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
                let mut follower_addition_ids = follower_ids
                    .difference(&user_state.followers)
                    .copied()
                    .collect::<Vec<_>>();
                let mut follower_removal_ids = user_state
                    .followers
                    .difference(&follower_ids)
                    .copied()
                    .collect::<Vec<_>>();
                let mut followed_addition_ids = following_ids
                    .difference(&user_state.following)
                    .copied()
                    .collect::<Vec<_>>();
                let mut followed_removal_ids = user_state
                    .following
                    .difference(&following_ids)
                    .copied()
                    .collect::<Vec<_>>();

                follower_addition_ids.sort_unstable();
                follower_removal_ids.sort_unstable();
                followed_addition_ids.sort_unstable();
                followed_removal_ids.sort_unstable();

                Batch::new(
                    Utc::now(),
                    user_id,
                    Some(Change::new(follower_addition_ids, follower_removal_ids)),
                    Some(Change::new(followed_addition_ids, followed_removal_ids)),
                )
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
    pub fn open<P: AsRef<Path>>(base: P) -> Result<Self, Error> {
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

    pub fn user_ids(&self) -> Vec<u64> {
        let mut user_ids = self
            .state
            .read()
            .unwrap()
            .users
            .keys()
            .copied()
            .collect::<Vec<_>>();
        user_ids.sort_unstable();
        user_ids
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

    pub fn past_batches(&self) -> PastBatchIterator<ZstFollowReader<'_>> {
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
                |error| PastBatchIterator::Failed(Some(error)),
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
    pub fn archive(&self) -> Result<Option<usize>, Error> {
        let mut state = self.state.write().unwrap();
        state.writer.flush()?;

        let file = File::open(self.current_file_path())?;
        let mut to_archive = HashMap::new();
        let mut current = vec![];
        let current_date = Utc::now().date_naive();

        for result in FollowReader::new(BufReader::new(file)) {
            let batch = result?;
            let batch_date = batch.timestamp.date_naive();

            if batch_date == current_date {
                current.push(batch);
            } else {
                to_archive
                    .entry(batch_date)
                    .or_insert_with(Vec::new)
                    .push(batch);
            }
        }

        if to_archive.is_empty() {
            Ok(None)
        } else {
            let mut by_path = to_archive
                .iter()
                .map(|(&date, batches)| (date, self.past_date_path(date), batches))
                .collect::<Vec<_>>();

            by_path.sort();

            if let Some(path) = by_path.iter().find_map(|(_, path, _)| {
                if path.exists() {
                    Some(path.clone())
                } else {
                    None
                }
            }) {
                Err(Error::PastFileCollision(path))
            } else {
                let mut archived_count = 0;

                for (_, path, batches) in by_path {
                    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
                    let mut writer = Encoder::new(file, ZSTD_LEVEL)?.auto_finish();

                    for batch in batches {
                        archived_count += 1;
                        write_batch(&mut writer, batch)?;
                    }
                }

                current.sort();

                let file = File::create(self.current_file_path())?;
                state.writer = BufWriter::new(file);

                for batch in current {
                    write_batch(&mut state.writer, &batch)?;
                }

                Ok(Some(archived_count))
            }
        }
    }

    /// Returns an unordered list of un-checked-out users and their last update time.
    pub fn user_updates(&self) -> Vec<(u64, DateTime<Utc>)> {
        let now = Utc::now();

        let state = self.state.read().unwrap();
        let mut user_ages = Vec::with_capacity(state.users.len());

        for (&id, user_state) in &state.users {
            if user_state
                .expiration
                .filter(|&expiration| expiration > now)
                .is_none()
            {
                user_ages.push((id, user_state.last_update));
            }
        }

        user_ages
    }

    /// Declares an intention to scrape this account, reserving it for an amount of time estimated
    /// from the given approximate follower and following count.
    ///
    /// Result will be empty if the account is already reserved.
    pub fn check_out(
        &self,
        user_id: u64,
        estimated_run_duration: Duration,
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
            user_state.expiration = Some(now + estimated_run_duration);
            Ok(Some(user_state.last_update))
        } else {
            Ok(None)
        }
    }

    pub fn undo_check_out(&self, user_id: u64, last_update: DateTime<Utc>) -> Result<bool, Error> {
        let mut state = self.state.write().unwrap();
        let user_state = state
            .users
            .get_mut(&user_id)
            .ok_or(Error::UntrackedId(user_id))?;

        if user_state.last_update == last_update {
            user_state.expiration = None;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn make_batch(
        &self,
        user_id: u64,
        follower_ids: HashSet<u64>,
        following_ids: HashSet<u64>,
    ) -> Batch {
        let state = self.state.read().unwrap();

        state.make_batch(user_id, follower_ids, following_ids)
    }

    pub fn update_and_write(&self, batch: &Batch, last_update: DateTime<Utc>) -> Result<(), Error> {
        let mut state = self.state.write().unwrap();
        state.update_and_write(batch, last_update)
    }

    pub fn validate(&self) -> Result<(), Error> {
        let mut last_timestamp = DateTime::<Utc>::MIN_UTC;
        let mut last_user_id = 0;

        self.state.write().unwrap().writer.flush()?;

        for result in self.past_batches() {
            let (date, batch) = result?;

            if date != batch.timestamp.date_naive() || last_timestamp > batch.timestamp {
                return Err(Error::InvalidBatch(batch));
            }

            if last_timestamp == batch.timestamp && last_user_id >= batch.user_id {
                return Err(Error::InvalidBatch(batch));
            }

            last_timestamp = batch.timestamp;
            last_user_id = batch.user_id;
        }

        let now_date = Utc::now().date_naive();

        for batch in self.current_batches()? {
            if batch.timestamp.date_naive() != now_date {
                return Err(Error::InvalidBatch(batch));
            }
        }

        Ok(())
    }
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
