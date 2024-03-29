//! A RocksDB database for storing user profiles from the Twitter API.

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use chrono::{DateTime, TimeZone, Utc};
use hst_tw_profiles::{avro::USER_SCHEMA, model::User};
use rocksdb::{DBCompressionType, IteratorMode, Options, DB};
use std::io::Cursor;
use std::iter::Peekable;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

pub mod table;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("UTF-8 decoding error")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("RocksDb error")]
    Db(#[from] rocksdb::Error),
    #[error("Avro decoding error")]
    Avro(#[from] apache_avro::Error),
    #[error("Invalid key bytes")]
    InvalidKeyBytes(Vec<u8>),
    #[error("Invalid timestamp bytes")]
    InvalidTimestampBytes(Vec<u8>),
    #[error("Invalid timestamp")]
    InvalidTimestamp(DateTime<Utc>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileDbCounts {
    pub id_count: u64,
    pub pair_count: u64,
}

#[derive(Clone)]
pub struct ProfileDb<M> {
    db: Arc<DB>,
    options: Options,
    mode: PhantomData<M>,
}

impl<M> table::Table for ProfileDb<M> {
    type Counts = ProfileDbCounts;

    fn underlying(&self) -> &DB {
        &self.db
    }

    fn get_counts(&self) -> Result<Self::Counts, Error> {
        let mut pair_count = 0;
        let mut id_count = 0;
        let mut last_id = 0;

        let iter = self.db.iterator(IteratorMode::Start);

        for result in iter {
            let (key, _) = result?;
            pair_count += 1;
            let (id, _) = key_to_pair(&key)?;
            if id != last_id {
                id_count += 1;
                last_id = id;
            }
        }

        Ok(Self::Counts {
            id_count,
            pair_count,
        })
    }
}

impl<M> ProfileDb<M> {
    pub fn statistics(&self) -> Option<String> {
        self.options.get_statistics()
    }

    pub fn lookup(&self, target_user_id: u64) -> Result<Vec<(DateTime<Utc>, User)>, Error> {
        let prefix = target_user_id.to_be_bytes();
        let iter = self.db.prefix_iterator(prefix);
        let mut users = vec![];

        for result in iter {
            let (key, value) = result?;
            let (user_id, snapshot) = key_to_pair(&key)?;

            if user_id == target_user_id {
                users.push((snapshot, parse_value(value)?));
            } else {
                break;
            }
        }

        Ok(users)
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = Result<(u64, Vec<(DateTime<Utc>, User)>), Error>> + '_ {
        ProfileIterator {
            underlying: self.raw_iter().peekable(),
        }
    }

    pub fn raw_iter(&self) -> impl Iterator<Item = Result<(u64, DateTime<Utc>, User), Error>> + '_ {
        self.db.iterator(IteratorMode::Start).map(|result| {
            result.map_err(Error::from).and_then(|(key, value)| {
                let (user_id, snapshot) = key_to_pair(&key)?;
                let user = parse_value(value)?;

                Ok((user_id, snapshot, user))
            })
        })
    }
}

impl<M: table::Mode> ProfileDb<M> {
    pub fn open<P: AsRef<Path>>(path: P, enable_statistics: bool) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::Zstd);

        if enable_statistics {
            options.enable_statistics();
        }

        let db = if M::is_read_only() {
            DB::open_for_read_only(&options, path, true)?
        } else {
            DB::open(&options, path)?
        };

        Ok(Self {
            db: Arc::new(db),
            options,
            mode: PhantomData,
        })
    }
}

impl ProfileDb<table::Writeable> {
    pub fn update(&self, user: &User) -> Result<(), Error> {
        let key = pair_to_key(user.id(), Utc.timestamp(user.snapshot, 0))?;
        let avro_value = to_value(user)?;
        let bytes = to_avro_datum(&USER_SCHEMA, avro_value)?;
        Ok(self.db.put(key, bytes)?)
    }
}

fn pair_to_key(user_id: u64, snapshot: DateTime<Utc>) -> Result<[u8; 12], Error> {
    let mut key = [0; 12];
    key[0..8].copy_from_slice(&user_id.to_be_bytes());

    let snapshot_s: u32 = snapshot
        .timestamp()
        .try_into()
        .map_err(|_| Error::InvalidTimestamp(snapshot))?;
    key[8..12].copy_from_slice(&snapshot_s.to_be_bytes());

    Ok(key)
}

fn key_to_pair(key: &[u8]) -> Result<(u64, DateTime<Utc>), Error> {
    let user_id = u64::from_be_bytes(
        key[0..8]
            .try_into()
            .map_err(|_| Error::InvalidKeyBytes(key.to_vec()))?,
    );
    let snapshot = u32::from_be_bytes(
        key[8..12]
            .try_into()
            .map_err(|_| Error::InvalidKeyBytes(key.to_vec()))?,
    );

    Ok((user_id, Utc.timestamp(snapshot as i64, 0)))
}

fn parse_value<T: AsRef<[u8]>>(value: T) -> Result<User, Error> {
    let mut cursor = Cursor::new(&value);
    let avro_value = from_avro_datum(&USER_SCHEMA, &mut cursor, None)?;
    Ok(from_value(&avro_value)?)
}

pub struct ProfileIterator<I: Iterator> {
    underlying: Peekable<I>,
}

impl<I: Iterator<Item = Result<(u64, DateTime<Utc>, User), Error>>> Iterator
    for ProfileIterator<I>
{
    type Item = Result<(u64, Vec<(DateTime<Utc>, User)>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            result.map(|(user_id, snapshot, user)| {
                let current_user_id = user_id;
                let mut users = vec![(snapshot, user)];

                while let Some(result) = self.underlying.next_if(|result| {
                    result
                        .as_ref()
                        .map(|(user_id, _, _)| *user_id == current_user_id)
                        .unwrap_or(false)
                }) {
                    // We've checked for errors just above, so this will always add a pair.
                    if let Ok((_, snapshot, user)) = result {
                        users.push((snapshot, user));
                    }
                }

                (current_user_id, users)
            })
        })
    }
}
