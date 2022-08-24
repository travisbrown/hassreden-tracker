use super::{DeactivationLog, Entry, Error};
use chrono::{DateTime, Utc};
use fd_lock::RwLock as FdLock;
use std::collections::HashMap;
use std::fs::File;
use std::io::Seek;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone, Debug)]
pub struct DeactivationFile {
    file: Arc<Mutex<FdLock<File>>>,
    log: Arc<RwLock<DeactivationLog>>,
}

impl DeactivationFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = File::options().read(true).write(true).open(&path)?;
        let log = DeactivationLog::read(&file)?;

        Ok(Self {
            file: Arc::new(Mutex::new(FdLock::new(file))),
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

    pub fn add_all(&self, updates: HashMap<u64, (u32, DateTime<Utc>)>) {
        let mut log = self.log.write().unwrap();
        log.add_all(updates);
    }

    pub fn flush(&self) -> Result<(), Error> {
        let log = self.log.read().unwrap();
        let mut mutex = self.file.lock().unwrap();
        let mut file = mutex.write()?;

        file.set_len(0)?;
        file.rewind()?;
        log.write(file.deref_mut())?;

        Ok(())
    }
}
