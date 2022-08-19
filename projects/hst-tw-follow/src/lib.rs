use chrono::{DateTime, Utc};

pub mod age;
pub mod formats;
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
