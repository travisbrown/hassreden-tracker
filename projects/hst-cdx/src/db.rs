use chrono::{DateTime, TimeZone, Utc};
use rocksdb::{DBCompressionType, Options, DB};
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
    #[error("Invalid value")]
    InvalidValue(Vec<u8>),
}

#[derive(Clone)]
pub struct DigestDb {
    db: Arc<DB>,
    options: Options,
}

impl DigestDb {
    pub fn open<P: AsRef<Path>>(path: P, enable_statistics: bool) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::Zstd);

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

    pub fn insert(&self, digest: &str, url: &str, archived_at: DateTime<Utc>) -> Result<(), Error> {
        let key = digest.as_bytes();
        let url_value = url.as_bytes();
        let mut value = Vec::with_capacity(url_value.len() + 8);
        value.extend_from_slice(&archived_at.timestamp().to_be_bytes());
        value.extend_from_slice(url_value);
        Ok(self.db.put(key, value)?)
    }

    pub fn lookup(&self, digest: &str) -> Result<Option<(String, DateTime<Utc>)>, Error> {
        let key = digest.as_bytes();

        let result = match self.db.get_pinned(key)? {
            Some(value) => {
                let timestamp_s = i64::from_be_bytes(
                    value[0..8]
                        .try_into()
                        .map_err(|_| Error::InvalidValue(value.to_vec()))?,
                );

                let timestamp = Utc.timestamp(timestamp_s, 0);
                let url = std::str::from_utf8(&value[8..])?.to_string();

                Some((url, timestamp))
            }
            None => None,
        };

        Ok(result)
    }

    pub fn lookup_batch(
        &self,
        digests: &[String],
    ) -> Result<Vec<Option<(String, DateTime<Utc>)>>, Error> {
        self.db
            .multi_get(digests.iter().map(|digest| digest.as_bytes()))
            .into_iter()
            .map(|result| {
                result.map_err(Error::from).and_then(|value| match value {
                    Some(value) => {
                        let timestamp_s = i64::from_be_bytes(
                            value[0..8]
                                .try_into()
                                .map_err(|_| Error::InvalidValue(value.to_vec()))?,
                        );

                        let timestamp = Utc.timestamp(timestamp_s, 0);
                        let url = std::str::from_utf8(&value[8..])?.to_string();

                        Ok(Some((url, timestamp)))
                    }
                    None => Ok(None),
                })
            })
            .collect::<Result<Vec<_>, Error>>()
    }
}
