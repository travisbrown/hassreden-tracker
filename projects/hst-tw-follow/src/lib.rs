use chrono::{DateTime, Utc};

pub mod archive;
pub mod db;
pub mod file;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Change {
    pub addition_ids: Vec<u64>,
    pub removal_ids: Vec<u64>,
}

impl Change {
    pub fn new(addition_ids: Vec<u64>, removal_ids: Vec<u64>) -> Self {
        Self {
            addition_ids,
            removal_ids,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Batch {
    pub timestamp: DateTime<Utc>,
    pub user_id: u64,
    pub follower_change: Change,
    pub followed_change: Change,
}

impl Batch {
    pub fn new(
        timestamp: DateTime<Utc>,
        user_id: u64,
        follower_change: Change,
        followed_change: Change,
    ) -> Self {
        Self {
            timestamp,
            user_id,
            follower_change,
            followed_change,
        }
    }
}
