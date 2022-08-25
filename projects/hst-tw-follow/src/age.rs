//! A RocksDB database for storing user profile ages.

use chrono::{DateTime, Duration, TimeZone, Utc};
use rocksdb::{Options, TransactionDB, TransactionDBOptions};
use std::path::Path;
use std::sync::Arc;

const AGE_TAG: u8 = 0;
const ID_TAG: u8 = 1;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("UTF-8 decoding error")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("RocksDb error")]
    Db(#[from] rocksdb::Error),
    #[error("Invalid key bytes")]
    InvalidKeyBytes(Vec<u8>),
    #[error("Invalid value bytes")]
    InvalidValueBytes(Vec<u8>),
    #[error("Invalid timestamp bytes")]
    InvalidTimestampBytes(Vec<u8>),
    #[error("Invalid timestamp")]
    InvalidTimestamp(DateTime<Utc>),
    #[error("Invalid duration")]
    InvalidDuration(Duration),
    #[error("Unexpected tag")]
    UnexpectedTag(u8),
}

#[derive(Clone)]
pub struct ProfileAgeDb {
    db: Arc<TransactionDB>,
    options: Options,
}

impl ProfileAgeDb {
    pub fn open<P: AsRef<Path>>(path: P, enable_statistics: bool) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);

        if enable_statistics {
            options.enable_statistics();
        }

        let transaction_options = TransactionDBOptions::default();

        let db = TransactionDB::open(&options, &transaction_options, path)?;

        Ok(Self {
            db: Arc::new(db),
            options,
        })
    }

    pub fn insert(
        &self,
        id: u64,
        last: Option<DateTime<Utc>>,
        target_age: Duration,
    ) -> Result<bool, Error> {
        let tx = self.db.transaction();
        let id_key = id_key(id);
        let next = last.map(|last| last + target_age);

        let replaced = match tx.get_pinned_for_update(id_key, true)? {
            Some(value) => {
                let (current_next, _) = parse_id_value(&value)?;
                tx.delete(age_key(current_next, id)?)?;
                true
            }
            None => false,
        };

        tx.put(age_key(next, id)?, age_value(last, None)?)?;
        tx.put(id_key, id_value(next, target_age)?)?;
        tx.commit()?;

        Ok(replaced)
    }

    pub fn prioritize(&self, id: u64, default_target_age: Duration) -> Result<bool, Error> {
        let tx = self.db.transaction();
        let id_key = id_key(id);

        let replaced = match tx.get_pinned_for_update(id_key, true)? {
            Some(value) => {
                let (current_next, target_age) = parse_id_value(&value)?;
                let current_age_key = age_key(current_next, id)?;
                let last = match tx.get_pinned_for_update(current_age_key, true)? {
                    Some(age_value) => {
                        let (last, _) = parse_age_value(&age_value)?;
                        last
                    }
                    None => None,
                };
                tx.delete(current_age_key)?;
                tx.put(age_key(None, id)?, age_value(last, None)?)?;
                tx.put(id_key, id_value(None, target_age)?)?;
                true
            }
            None => {
                tx.put(age_key(None, id)?, age_value(None, None)?)?;
                tx.put(id_key, id_value(None, default_target_age)?)?;
                false
            }
        };

        tx.commit()?;

        Ok(replaced)
    }

    pub fn finish(&self, id: u64, default_target_age: Duration) -> Result<bool, Error> {
        let tx = self.db.transaction();
        let id_key = id_key(id);
        let now = Utc::now();

        let replaced = match tx.get_pinned_for_update(id_key, true)? {
            Some(value) => {
                let (current_next, target_age) = parse_id_value(&value)?;
                tx.delete(age_key(current_next, id)?)?;

                let new_next = now + target_age;

                tx.put(age_key(Some(new_next), id)?, age_value(Some(now), None)?)?;
                tx.put(id_key, id_value(Some(new_next), target_age)?)?;
                true
            }
            None => {
                let new_next = now + default_target_age;

                tx.put(age_key(Some(new_next), id)?, age_value(Some(now), None)?)?;
                tx.put(id_key, id_value(Some(new_next), default_target_age)?)?;
                false
            }
        };

        tx.commit()?;

        Ok(replaced)
    }

    /// The account has been deactivated or suspended.
    pub fn delete(&self, id: u64) -> Result<bool, Error> {
        let tx = self.db.transaction();
        let id_key = id_key(id);

        let removed = match tx.get_pinned_for_update(id_key, true)? {
            Some(value) => {
                let (current_next, _) = parse_id_value(&value)?;
                tx.delete(age_key(current_next, id)?)?;
                tx.delete(id_key)?;
                true
            }
            None => false,
        };

        tx.commit()?;

        Ok(removed)
    }

    pub fn queue_status(&self) -> Result<(usize, DateTime<Utc>), Error> {
        let iter = self.db.prefix_iterator([AGE_TAG]);
        let mut prioritized_count = 0;
        let mut past_prioritized = false;
        let mut first_next = DateTime::<Utc>::MIN_UTC;

        for result in iter {
            let (key, value) = result?;

            if key[0] == AGE_TAG {
                let (next, _) = parse_age_key(&key)?;
                let (_, started) = parse_age_value(&value)?;

                if started.is_none() {
                    match next {
                        Some(next) => {
                            first_next = next;
                            past_prioritized = true;
                            break;
                        }
                        None => {
                            if !past_prioritized {
                                prioritized_count += 1;
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        Ok((prioritized_count, first_next))
    }

    pub fn get_next(
        &self,
        count: usize,
        min_age: Duration,
        min_running: Duration,
    ) -> Result<Vec<u64>, Error> {
        let tx = self.db.transaction();
        let iter = tx.prefix_iterator([AGE_TAG]);
        let now = Utc::now();

        let pairs = iter
            .map(|result| {
                result.map_err(Error::from).and_then(|(key, value)| {
                    if key[0] == AGE_TAG {
                        let (_, id) = parse_age_key(&key)?;
                        let (last, started) = parse_age_value(&value)?;

                        Ok(Some((key, id, last, started)))
                    } else {
                        Ok(None)
                    }
                })
            })
            .take_while(|result| {
                result
                    .as_ref()
                    .map_or_else(|_| true, |value| value.is_some())
            })
            .filter_map(|result| {
                result.map_or_else(
                    |error| Some(Err(error)),
                    |value|
                        value.and_then(|(key, id, last, started)|
                            // Ignore old last snapshots.
                            if last.filter(|last| now - *last < min_age).is_some() {
                                // The last snapshot is too new.
                                None
                            } else if started.filter(|started| now - *started < min_running).is_some() {
                                // The current run is too new.
                                None
                            } else {
                                Some(Ok((key, id, last)))
                            }
                        )
                )
            })
            .take(count)
            .collect::<Result<Vec<_>, _>>()?;

        let now = Utc::now();
        let mut ids = Vec::with_capacity(pairs.len());

        for (age_key, id, last) in pairs {
            tx.put(age_key, age_value(last, Some(now))?)?;
            ids.push(id);
        }

        tx.commit()?;

        Ok(ids)
    }

    /// Only for debugging.
    pub fn dump_next(
        &self,
        count: usize,
    ) -> Result<
        Vec<(
            u64,
            Option<DateTime<Utc>>,
            Option<DateTime<Utc>>,
            Option<DateTime<Utc>>,
        )>,
        Error,
    > {
        let tx = self.db.transaction();
        let iter = tx.prefix_iterator([AGE_TAG]);

        let items = iter
            .map(|result| {
                result.map_err(Error::from).and_then(|(key, value)| {
                    if key[0] == AGE_TAG {
                        let (next, id) = parse_age_key(&key)?;
                        let (last, started) = parse_age_value(&value)?;

                        Ok(Some((id, next, last, started)))
                    } else {
                        Ok(None)
                    }
                })
            })
            .take_while(|result| {
                result
                    .as_ref()
                    .map_or_else(|_| true, |value| value.is_some())
            })
            .filter_map(|result| {
                result.map_or_else(|error| Some(Err(error)), |value| value.map(Ok))
            })
            .take(count)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(items)
    }
}

fn age_key(next: Option<DateTime<Utc>>, id: u64) -> Result<[u8; 13], Error> {
    let mut key = [0; 13];
    key[0] = AGE_TAG;

    let next_s: u32 = timestamp_to_u32(next)?;

    key[1..5].copy_from_slice(&next_s.to_be_bytes());
    key[5..13].copy_from_slice(&id.to_be_bytes());

    Ok(key)
}

fn parse_age_key(key: &[u8]) -> Result<(Option<DateTime<Utc>>, u64), Error> {
    if key[0] != AGE_TAG {
        Err(Error::UnexpectedTag(key[0]))
    } else {
        let next_s = u32::from_be_bytes(
            key[1..5]
                .try_into()
                .map_err(|_| Error::InvalidKeyBytes(key.to_vec()))?,
        );

        let id = u64::from_be_bytes(
            key[5..13]
                .try_into()
                .map_err(|_| Error::InvalidKeyBytes(key.to_vec()))?,
        );

        Ok((timestamp_from_u32(next_s), id))
    }
}

fn age_value(
    last: Option<DateTime<Utc>>,
    started: Option<DateTime<Utc>>,
) -> Result<Vec<u8>, Error> {
    let mut value = Vec::with_capacity(8);

    let last_s = timestamp_to_u32(last)?;
    value.extend_from_slice(&last_s.to_be_bytes());

    if started.is_some() {
        let started_s = timestamp_to_u32(started)?;
        value.extend_from_slice(&started_s.to_be_bytes());
    }

    Ok(value)
}

fn parse_age_value(value: &[u8]) -> Result<(Option<DateTime<Utc>>, Option<DateTime<Utc>>), Error> {
    match value.len() {
        4 => {
            let last = parse_timestamp_value(value)?;
            Ok((last, None))
        }
        8 => {
            let last = parse_timestamp_value(&value[0..4])?;
            let started = parse_timestamp_value(&value[4..8])?;
            Ok((last, started))
        }
        _ => Err(Error::InvalidValueBytes(value.to_vec())),
    }
}

fn id_key(id: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = ID_TAG;
    key[1..9].copy_from_slice(&id.to_be_bytes());
    key
}

fn id_value(next: Option<DateTime<Utc>>, target_age: Duration) -> Result<[u8; 8], Error> {
    let mut value = [0; 8];
    let target_age_s =
        u32::try_from(target_age.num_seconds()).map_err(|_| Error::InvalidDuration(target_age))?;

    value[0..4].copy_from_slice(&timestamp_to_u32(next)?.to_be_bytes());
    value[4..8].copy_from_slice(&target_age_s.to_be_bytes());
    Ok(value)
}

fn parse_id_value(value: &[u8]) -> Result<(Option<DateTime<Utc>>, Duration), Error> {
    if value.len() == 8 {
        let next = parse_timestamp_value(&value[0..4])?;
        let target_age_s = u32::from_be_bytes(
            value[4..8]
                .try_into()
                .map_err(|_| Error::InvalidValueBytes(value.to_vec()))?,
        );
        Ok((next, Duration::seconds(target_age_s as i64)))
    } else {
        Err(Error::InvalidValueBytes(value.to_vec()))
    }
}

fn parse_timestamp_value(value: &[u8]) -> Result<Option<DateTime<Utc>>, Error> {
    if value.is_empty() {
        Ok(None)
    } else {
        let timestamp_s = u32::from_be_bytes(
            value
                .try_into()
                .map_err(|_| Error::InvalidValueBytes(value.to_vec()))?,
        );

        Ok(timestamp_from_u32(timestamp_s))
    }
}

fn timestamp_to_u32(timestamp: Option<DateTime<Utc>>) -> Result<u32, Error> {
    let timestamp_s = match timestamp {
        Some(timestamp) => timestamp
            .timestamp()
            .try_into()
            .map_err(|_| Error::InvalidTimestamp(timestamp))?,
        None => 0,
    };

    Ok(timestamp_s)
}

fn timestamp_from_u32(value: u32) -> Option<DateTime<Utc>> {
    if value == 0 {
        None
    } else {
        Some(Utc.timestamp(value as i64, 0))
    }
}
