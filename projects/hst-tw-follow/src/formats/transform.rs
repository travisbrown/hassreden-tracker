use super::super::Batch;
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::iter::Peekable;

pub fn deduplicate_removals<E, I: Iterator<Item = Result<Batch, E>>>(
    batches: I,
) -> RemovalDeduplicator<I> {
    RemovalDeduplicator {
        underlying: batches,
        users: HashMap::new(),
    }
}

pub fn partition_dates<E, I: Iterator<Item = Result<Batch, E>>>(batches: I) -> DatePartitioner<I> {
    DatePartitioner {
        underlying: batches.peekable(),
    }
}

/// Remove duplicated removal entries.
pub struct RemovalDeduplicator<I> {
    underlying: I,
    users: HashMap<u64, (HashSet<u64>, HashSet<u64>)>,
}

impl<E, I: Iterator<Item = Result<Batch, E>>> Iterator for RemovalDeduplicator<I> {
    type Item = Result<Batch, E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            let mut batch = result?;
            let (followers, following) = self.users.entry(batch.user_id).or_default();

            if let Some(ref mut change) = batch.follower_change {
                change.removal_ids.retain(|id| followers.remove(id));
                change.addition_ids.iter().for_each(|id| {
                    followers.insert(*id);
                });
            }

            if let Some(ref mut change) = batch.followed_change {
                change.removal_ids.retain(|id| following.remove(id));
                change.addition_ids.iter().for_each(|id| {
                    following.insert(*id);
                });
            }

            Ok(batch)
        })
    }
}

pub struct DatePartitioner<I: Iterator> {
    underlying: Peekable<I>,
}

impl<E, I: Iterator<Item = Result<Batch, E>>> Iterator for DatePartitioner<I> {
    type Item = Result<(NaiveDate, Vec<Batch>), E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            let batch = result?;
            let date = batch.timestamp.date_naive();
            let mut batches = vec![batch];

            while let Some(next) = self.underlying.next_if(|result| {
                result
                    .as_ref()
                    .map_or(false, |batch| batch.timestamp.date_naive() == date)
            }) {
                // We've just checked for failure so this will always add an element.
                if let Ok(batch) = next {
                    batches.push(batch);
                }
            }

            Ok((date, batches))
        })
    }
}
