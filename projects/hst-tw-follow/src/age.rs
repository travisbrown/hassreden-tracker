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

    pub fn update(
        &self,
        id: u64,
        last: Option<DateTime<Utc>>,
        next: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let tx = self.db.transaction();
        let id_key = id_key(id);

        match tx.get_pinned_for_update(id_key, true)? {
            Some(value) => {
                let current_next = parse_id_value(&value)?;
                tx.delete(age_key(current_next, id)?)?;
                tx.put(age_key(next, id)?, age_value(last, None)?)?;
                tx.put(id_key, id_value(next)?)?;
            }
            None => {
                // We haven't seen this ID, so we automatically promote it to urgent status.
                tx.put(age_key(None, id)?, age_value(last, None)?)?;
                tx.put(id_key, id_value(None)?)?;
            }
        };

        Ok(tx.commit()?)
    }

    /// The account has been deactivated or suspended.
    pub fn delete(&self, id: u64) -> Result<bool, Error> {
        let tx = self.db.transaction();
        let id_key = id_key(id);

        let removed = match tx.get_pinned_for_update(id_key, true)? {
            Some(value) => {
                let current_next = parse_id_value(&value)?;
                tx.delete(age_key(current_next, id)?)?;
                tx.delete(id_key)?;
                true
            }
            None => false,
        };

        tx.commit()?;

        Ok(removed)
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

                        // The last snapshot is too new.
                        if last.filter(|last| now - *last < min_age).is_some() {
                            Ok(None)
                        } else {
                            Ok(Some((key, id, started)))
                        }
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
                    |value| {
                        value.and_then(|(key, id, started)| match started {
                            Some(started) => {
                                // The currently run is too new
                                if now - started < min_running {
                                    None
                                } else {
                                    Some(Ok((key, id)))
                                }
                            }
                            None => Some(Ok((key, id))),
                        })
                    },
                )
            })
            .take(count)
            .collect::<Result<Vec<_>, _>>()?;

        let value = timestamp_to_u32(Some(now))?;

        for (key, _) in pairs {
            tx.put(key, value.to_be_bytes())?;
        }

        tx.commit()?;

        Ok(vec![])
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

fn age_key(snapshot: Option<DateTime<Utc>>, id: u64) -> Result<[u8; 13], Error> {
    let mut key = [0; 13];
    key[0] = AGE_TAG;

    let snapshot_s: u32 = timestamp_to_u32(snapshot)?;

    key[1..5].copy_from_slice(&snapshot_s.to_be_bytes());
    key[5..13].copy_from_slice(&id.to_be_bytes());

    Ok(key)
}

fn parse_age_key(key: &[u8]) -> Result<(Option<DateTime<Utc>>, u64), Error> {
    if key[0] != AGE_TAG {
        Err(Error::UnexpectedTag(key[0]))
    } else {
        let snapshot_s = u32::from_be_bytes(
            key[1..5]
                .try_into()
                .map_err(|_| Error::InvalidKeyBytes(key.to_vec()))?,
        );

        let id = u64::from_be_bytes(
            key[5..13]
                .try_into()
                .map_err(|_| Error::InvalidKeyBytes(key.to_vec()))?,
        );

        Ok((timestamp_from_u32(snapshot_s), id))
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

fn id_value(next: Option<DateTime<Utc>>) -> Result<[u8; 4], Error> {
    Ok(timestamp_to_u32(next)?.to_be_bytes())
}

fn parse_id_value(value: &[u8]) -> Result<Option<DateTime<Utc>>, Error> {
    if value.len() == 4 {
        let next = parse_timestamp_value(&value[0..4])?;
        Ok(next)
    } else {
        Err(Error::InvalidValueBytes(value.to_vec()))
    }
}

fn timestamp_value(timestamp: Option<DateTime<Utc>>) -> Result<Vec<u8>, Error> {
    match timestamp {
        Some(timestamp) => {
            let timestamp_s = timestamp_to_u32(Some(timestamp))?;
            Ok(timestamp_s.to_be_bytes().to_vec())
        }
        None => Ok(vec![]),
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
