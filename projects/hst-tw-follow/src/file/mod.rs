use super::{Batch, Change};
use std::path::Path;

pub mod read;
pub mod write;

pub(crate) const FOLLOWERS_DIR_NAME: &str = "followers";
pub(crate) const FOLLOWING_DIR_NAME: &str = "following";

pub fn read_batches<P: AsRef<Path>>(base: P) -> read::BatchIterator {
    read::BatchIterator::new(base)
}

pub use write::write_batches;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Invalid user directory")]
    InvalidUserDirectory(Box<Path>),
    #[error("Invalid batch file")]
    InvalidBatchFile(Box<Path>),
    #[error("Invalid update line")]
    InvalidUpdateLine(String),
    #[error("Invalid timestamp")]
    InvalidTimestamp(String),
}
