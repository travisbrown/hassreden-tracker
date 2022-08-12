use chrono::{DateTime, Utc};

pub mod archive;
pub mod db;
pub mod file;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Change {
    pub addition_ids: Vec<u64>,
    pub removal_ids: Vec<u64>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Batch {
    pub timestamp: DateTime<Utc>,
    pub user_id: u64,
    pub follower_change: Change,
    pub followed_change: Change,
}
