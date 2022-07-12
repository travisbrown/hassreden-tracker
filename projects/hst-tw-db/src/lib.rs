use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use chrono::{DateTime, TimeZone, Utc};
use hst_tw_profiles::{avro::USER_SCHEMA, model::User};
use rocksdb::{DBIterator, IteratorMode, MergeOperands, Options, DB};
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

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
    #[error("Invalid key")]
    InvalidKey(Vec<u8>),
    #[error("Invalid timestamp")]
    InvalidTimestamp(Vec<u8>),
}

#[derive(Clone)]
pub struct ProfileDb {
    db: Arc<DB>,
    options: Options,
}

impl ProfileDb {
    pub fn open<P: AsRef<Path>>(path: P, enable_statistics: bool) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator_associative("merge", merge);

        if enable_statistics {
            options.enable_statistics();
        }

        let db = DB::open(&options, path)?;

        Ok(Self {
            db: Arc::new(db),
            options,
        })
    }

    pub fn estimate_key_count(&self) -> Result<usize, Error> {
        let value = self.db.property_int_value("rocksdb.estimate-num-keys")?;

        Ok(value.map(|value| value as usize).unwrap_or_default())
    }

    pub fn statistics(&self) -> Option<String> {
        self.options.get_statistics()
    }

    pub fn lookup(&self, user_id: u64) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>, User)>, Error> {
        let prefix = user_id.to_be_bytes();
        let iterator = self.db.prefix_iterator(prefix);
        let mut users: Vec<(DateTime<Utc>, DateTime<Utc>, User)> = vec![];

        for (key, value) in iterator {
            let next_user_id = u64::from_be_bytes(
                key[0..8]
                    .try_into()
                    .map_err(|_| Error::InvalidKey(key.to_vec()))?,
            );

            if next_user_id == user_id {
                users.push(parse_value(value)?);
            } else {
                break;
            }
        }

        users.sort_by_key(|(_, _, user)| user.snapshot);

        Ok(users)
    }

    pub fn iter(&self) -> ProfileIterator<DBIterator> {
        ProfileIterator {
            underlying: self
                .db
                .iterator(IteratorMode::From(&[], rocksdb::Direction::Forward)),
            current: None,
            finished: false,
        }
    }

    pub fn raw_iter(
        &self,
    ) -> impl Iterator<Item = Result<(u64, (DateTime<Utc>, DateTime<Utc>, User)), Error>> + '_ {
        self.db
            .iterator(IteratorMode::From(&[], rocksdb::Direction::Forward))
            .map(|(key, value)| {
                let user_id = u64::from_be_bytes(
                    key[0..8]
                        .try_into()
                        .map_err(|_| Error::InvalidKey(key.to_vec()))?,
                );

                let result = parse_value(value)?;

                Ok((user_id, result))
            })
    }

    pub fn update(&self, user: &User) -> Result<(), Error> {
        let key = Self::make_key(user.id, &user.screen_name);
        let avro_value = to_value(user)?;
        let bytes = to_avro_datum(&USER_SCHEMA, avro_value)?;
        let mut value = Vec::with_capacity(bytes.len() + 8);
        value.extend_from_slice(&user.snapshot.to_be_bytes());
        value.extend_from_slice(&user.snapshot.to_be_bytes());
        value.extend_from_slice(&bytes);
        Ok(self.db.merge(key, value)?)
    }

    fn make_key(user_id: i64, screen_name: &str) -> Vec<u8> {
        let screen_name_clean = screen_name.to_lowercase();
        let screen_name_bytes = screen_name_clean.as_bytes();
        let mut key = Vec::with_capacity(screen_name_bytes.len() + 8);
        key.extend_from_slice(&user_id.to_be_bytes());
        key.extend_from_slice(screen_name_bytes);
        key
    }
}

pub struct ProfileIterator<I> {
    underlying: I,
    current: Option<(DateTime<Utc>, DateTime<Utc>, User)>,
    finished: bool,
}

impl<I: Iterator<Item = (Box<[u8]>, Box<[u8]>)>> Iterator for ProfileIterator<I> {
    type Item = Result<Vec<(DateTime<Utc>, DateTime<Utc>, User)>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.take() {
            Some((first_timestamp, last_timestamp, user)) => {
                let user_id = user.id;
                let mut batch = vec![(first_timestamp, last_timestamp, user)];

                loop {
                    match self.underlying.next() {
                        Some((_, value)) => match parse_value(value) {
                            Ok((first_timestamp, last_timestamp, next_user)) => {
                                if next_user.id == user_id {
                                    batch.push((first_timestamp, last_timestamp, next_user));
                                } else {
                                    self.current =
                                        Some((first_timestamp, last_timestamp, next_user));
                                    batch.sort_by_key(|(_, _, user)| user.snapshot);
                                    return Some(Ok(batch));
                                }
                            }
                            Err(error) => {
                                self.finished = true;
                                return Some(Err(error));
                            }
                        },
                        None => {
                            self.finished = true;
                            return Some(Ok(batch));
                        }
                    };
                }
            }
            None => {
                if self.finished {
                    None
                } else {
                    match self.underlying.next() {
                        Some((_, value)) => match parse_value(value) {
                            Ok((first_timestamp, last_timestamp, next_user)) => {
                                self.current = Some((first_timestamp, last_timestamp, next_user));
                                self.next()
                            }
                            Err(error) => {
                                self.finished = true;
                                Some(Err(error))
                            }
                        },
                        None => None,
                    }
                }
            }
        }
    }
}

fn parse_timestamps<T: AsRef<[u8]>>(value: T) -> Result<(i64, i64), Error> {
    let value = value.as_ref();
    let first_timestamp_s = i64::from_be_bytes(
        value[0..8]
            .try_into()
            .map_err(|_| Error::InvalidTimestamp(value[0..8].to_vec()))?,
    );
    let last_timestamp_s = i64::from_be_bytes(
        value[8..16]
            .try_into()
            .map_err(|_| Error::InvalidTimestamp(value[8..16].to_vec()))?,
    );

    Ok((first_timestamp_s, last_timestamp_s))
}

fn parse_value<T: AsRef<[u8]>>(value: T) -> Result<(DateTime<Utc>, DateTime<Utc>, User), Error> {
    let (first_timestamp_s, last_timestamp_s) = parse_timestamps(&value)?;

    let value = value.as_ref();
    let mut cursor = Cursor::new(&value[16..]);
    let avro_value = from_avro_datum(&USER_SCHEMA, &mut cursor, None)?;
    let user = from_value(&avro_value)?;
    Ok((
        Utc.timestamp(first_timestamp_s, 0),
        Utc.timestamp(last_timestamp_s, 0),
        user,
    ))
}

fn merge(_key: &[u8], existing_val: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut current_first_timestamp_s = None;
    let mut current_last_timestamp_s = None;
    let mut current_value = None;

    if let Some(bytes) = existing_val {
        match parse_timestamps(bytes) {
            Ok((first_timestamp_s, last_timestamp_s)) => {
                current_first_timestamp_s = Some(first_timestamp_s);
                current_last_timestamp_s = Some(last_timestamp_s);
                current_value = Some(bytes);
            }
            Err(error) => {
                log::error!("Merge error: {:?}", error);
            }
        }
    }

    for bytes in operands.into_iter() {
        match parse_timestamps(bytes) {
            Ok((first_timestamp_s, last_timestamp_s)) => {
                match current_first_timestamp_s {
                    Some(previous_first_timestamp_s)
                        if first_timestamp_s < previous_first_timestamp_s =>
                    {
                        current_first_timestamp_s = Some(first_timestamp_s);
                    }
                    None => {
                        current_first_timestamp_s = Some(first_timestamp_s);
                    }
                    _ => (),
                }
                match current_last_timestamp_s {
                    Some(previous_last_timestamp_s)
                        if last_timestamp_s > previous_last_timestamp_s =>
                    {
                        current_last_timestamp_s = Some(last_timestamp_s);
                        current_value = Some(bytes);
                    }
                    None => {
                        current_last_timestamp_s = Some(last_timestamp_s);
                        current_value = Some(bytes);
                    }
                    _ => (),
                }
            }
            Err(error) => {
                log::error!("Merge error: {:?}", error);
            }
        }
    }

    match (current_first_timestamp_s, current_value) {
        (Some(first_timestamp_s), Some(value)) => {
            let mut new_value = value.to_vec();
            new_value.splice(0..8, first_timestamp_s.to_be_bytes());

            Some(new_value)
        }
        _ => {
            log::error!("Unexpected merge values");
            existing_val.map(|bytes| bytes.to_vec())
        }
    }
}
