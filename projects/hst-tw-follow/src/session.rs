use crate::{
    dbs::tracked::{TrackedUser, TrackedUserDb},
    store::Store,
    Batch,
};
use chrono::{Duration, Utc};
use egg_mode_extras::{client::TokenType, Client};
use hst_deactivations::file::DeactivationFile;
use std::collections::HashMap;
use std::path::Path;

const RUN_DURATION_BUFFER_S: i64 = 20 * 60;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Twitter API client error")]
    EggModeExtras(#[from] egg_mode_extras::error::Error),
    #[error("Store error")]
    Store(#[from] crate::store::Error),
    #[error("Duplicate user ID")]
    TrackedUserDb(#[from] crate::dbs::tracked::Error),
    #[error("Deactivations file error")]
    Deactivations(#[from] hst_deactivations::Error),
}

#[derive(Debug)]
pub enum RunInfo {
    Archived {
        archived_batch_count: usize,
    },
    Scraped {
        batch: Batch,
    },
    /// Indicates that we need to check whether the user is protected or blocks our account.
    UnavailableUser {
        id: u64,
    },
    Next(u64, Duration),
}

pub struct Session {
    twitter_client: Client,
    store: Store,
    tracked: TrackedUserDb,
    deactivations: DeactivationFile,
}

impl Session {
    pub fn open<P: AsRef<Path>>(
        twitter_client: Client,
        store_path: P,
        tracked_path: P,
        deactivations_path: P,
    ) -> Result<Self, Error> {
        let store = Store::open(store_path)?;
        let tracked = TrackedUserDb::open(tracked_path)?;
        let deactivations = DeactivationFile::open(deactivations_path)?;

        Ok(Self {
            twitter_client,
            store,
            tracked,
            deactivations,
        })
    }

    pub async fn run(&self, token_type: TokenType) -> Result<Option<RunInfo>, Error> {
        if let Some(archived_batch_count) = self.store.archive()? {
            Ok(Some(RunInfo::Archived {
                archived_batch_count,
            }))
        } else {
            let user_updates = self.store.user_updates();
            let tracked_users = self
                .tracked
                .users()?
                .into_iter()
                .map(|user| (user.id, user))
                .collect::<HashMap<_, _>>();

            let mut candidates = vec![];
            let now = Utc::now();

            for (id, last_update) in user_updates {
                if self.deactivations.status(id).is_none() {
                    let user = tracked_users.get(&id);

                    if !user
                        .map(|user| {
                            user.protected || check_block(&self.twitter_client, user, token_type)
                        })
                        .unwrap_or(false)
                    {
                        let age = now - last_update;
                        let target_age = user
                            .map(|user| {
                                user.target_age
                                    .unwrap_or_else(|| default_target_age(user.followers_count))
                            })
                            .unwrap_or_else(|| default_target_age(15_000));

                        candidates.push((id, user, age - target_age));
                    }
                }
            }

            match candidates
                .into_iter()
                .max_by_key(|(id, _, diff)| (*diff, *id))
            {
                Some((id, user, diff)) => {
                    self.store.check_out(
                        id,
                        estimate_run_duration(
                            user.map(|user| user.followers_count).unwrap_or(15_000),
                        ),
                    )?;
                    Ok(Some(RunInfo::Next(id, diff)))
                }
                None => Ok(None),
            }
        }
    }
}

fn check_block(client: &Client, user: &TrackedUser, token_type: TokenType) -> bool {
    token_type == TokenType::User && user.blocks.contains(&client.user_id())
}

fn default_target_age(followers_count: usize) -> Duration {
    let min_count = 15_000;
    let max_count = 1_000_000;
    let count_range = max_count - min_count;
    let min_target_age = Duration::days(2);
    let max_target_age = Duration::days(30);
    let unit = (max_target_age - min_target_age) / count_range as i32;

    if followers_count >= max_count {
        max_target_age
    } else if followers_count <= min_count {
        min_target_age
    } else {
        min_target_age + unit * (followers_count - min_count) as i32
    }
}

fn estimate_run_duration(count: usize) -> Duration {
    Duration::seconds((((count / 75000) + 1) * 15 * 60) as i64)
        + Duration::seconds(RUN_DURATION_BUFFER_S)
}
