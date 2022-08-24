use chrono::{DateTime, Utc};
use std::collections::HashSet;

pub mod age;
pub mod dbs;
pub mod downloader;
pub mod formats;
pub mod session;
pub mod store;

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

    pub fn total_len(&self) -> usize {
        self.addition_ids.len() + self.removal_ids.len()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Batch {
    pub timestamp: DateTime<Utc>,
    pub user_id: u64,
    pub follower_change: Option<Change>,
    pub followed_change: Option<Change>,
}

impl Batch {
    pub fn new(
        timestamp: DateTime<Utc>,
        user_id: u64,
        follower_change: Option<Change>,
        followed_change: Option<Change>,
    ) -> Self {
        Self {
            timestamp,
            user_id,
            follower_change,
            followed_change,
        }
    }

    pub fn addition_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::new();

        if let Some(change) = &self.follower_change {
            for &id in &change.addition_ids {
                ids.insert(id);
            }
        }

        if let Some(change) = &self.followed_change {
            for &id in &change.addition_ids {
                ids.insert(id);
            }
        }

        ids
    }

    pub fn removal_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::new();

        if let Some(change) = &self.follower_change {
            for &id in &change.removal_ids {
                ids.insert(id);
            }
        }

        if let Some(change) = &self.followed_change {
            for &id in &change.removal_ids {
                ids.insert(id);
            }
        }

        ids
    }

    pub fn total_len(&self) -> usize {
        self.follower_change
            .as_ref()
            .map_or_else(|| 0, |change| change.total_len())
            + self
                .followed_change
                .as_ref()
                .map_or_else(|| 0, |change| change.total_len())
    }
}
