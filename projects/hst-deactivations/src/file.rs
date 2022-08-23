use super::{DeactivationLog, Entry, Error};
use chrono::{DateTime, Utc};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub struct DeactivationFile {
    path: Box<Path>,
    log: Arc<RwLock<DeactivationLog>>,
}

impl DeactivationFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = File::open(&path)?;
        let log = DeactivationLog::read(file)?;

        Ok(Self {
            path: path.as_ref().to_path_buf().into_boxed_path(),
            log: Arc::new(RwLock::new(log)),
        })
    }

    pub fn log(&self) -> DeactivationLog {
        let log = self.log.read().unwrap();
        log.clone()
    }

    pub fn lookup(&self, user_id: u64) -> Option<Vec<Entry>> {
        let log = self.log.read().unwrap();
        log.lookup(user_id)
    }

    pub fn status(&self, user_id: u64) -> Option<u32> {
        let log = self.log.read().unwrap();
        log.status(user_id)
    }

    pub fn add(&self, user_id: u64, status: u32, observed: DateTime<Utc>) {
        let mut log = self.log.write().unwrap();
        log.add(user_id, status, observed);
    }

    pub fn flush(&self) -> Result<(), Error> {
        let log = self.log.write().unwrap();
        let file = File::create(&self.path)?;

        Ok(log.write(file)?)
    }
}
