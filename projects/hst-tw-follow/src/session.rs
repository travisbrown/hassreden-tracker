use crate::{
    dbs::tracked::{TrackedUser, TrackedUserDb},
    store::Store,
    Batch,
};
use chrono::{Duration, Utc};
use egg_mode_extras::{client::TokenType, error::UnavailableReason, Client};
use futures::{future::TryFutureExt, stream::TryStreamExt};
use hst_deactivations::file::DeactivationFile;
use std::collections::{HashMap, HashSet};
use std::path::Path;

const RUN_DURATION_BUFFER_S: i64 = 20 * 60;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Twitter API error")]
    EggMode(#[from] egg_mode::error::Error),
    #[error("Twitter API client error")]
    EggModeExtras(#[from] egg_mode_extras::error::Error),
    #[error("Store error")]
    Store(#[from] crate::store::Error),
    #[error("Duplicate user ID")]
    TrackedUserDb(#[from] crate::dbs::tracked::Error),
    #[error("Deactivations file error")]
    Deactivations(#[from] hst_deactivations::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnavailableStatus {
    Block,
    Deactivated,
    Suspended,
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
    Unavailable {
        id: u64,
        status: UnavailableStatus,
    },
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
            let mut tracked_users = self
                .tracked
                .users()?
                .into_iter()
                .map(|user| (user.id, user))
                .collect::<HashMap<_, _>>();

            let mut candidates = vec![];
            let now = Utc::now();

            for (id, last_update) in user_updates {
                if self.deactivations.status(id).is_none() {
                    let user = tracked_users.remove(&id);

                    if !user
                        .as_ref()
                        .map(|user| {
                            user.protected || check_block(&self.twitter_client, &user, token_type)
                        })
                        .unwrap_or(false)
                    {
                        let age = now - last_update;
                        let target_age = user
                            .as_ref()
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
                Some((id, user, _)) => self.scrape(token_type, id, user).await,
                None => Ok(None),
            }
        }
    }

    pub async fn scrape(
        &self,
        token_type: TokenType,
        user_id: u64,
        user: Option<TrackedUser>,
    ) -> Result<Option<RunInfo>, Error> {
        let user = match user {
            Some(user) => Some(user),
            None => self.tracked.get(user_id)?,
        };

        if let Some(last_update) = self.store.check_out(
            user_id,
            estimate_run_duration(user.map(|user| user.followers_count).unwrap_or(15_000)),
        )? {
            match scrape_follows(&self.twitter_client, token_type, user_id).await? {
                Ok((follower_ids, followed_ids)) => {
                    let batch = self.store.make_batch(user_id, follower_ids, followed_ids);
                    self.store.update_and_write(&batch, last_update)?;

                    Ok(Some(RunInfo::Scraped { batch }))
                }
                Err(status) => {
                    match status {
                        UnavailableStatus::Block => {
                            self.tracked
                                .put_block(user_id, self.twitter_client.user_id())?;
                        }
                        UnavailableStatus::Deactivated => {
                            self.deactivations.add(user_id, 50, Utc::now());
                            self.deactivations.flush()?;
                        }
                        UnavailableStatus::Suspended => {
                            self.deactivations.add(user_id, 63, Utc::now());
                            self.deactivations.flush()?;
                        }
                    }

                    self.store.undo_check_out(user_id, last_update)?;

                    Ok(Some(RunInfo::Unavailable {
                        id: user_id,
                        status,
                    }))
                }
            }
        } else {
            Ok(None)
        }
    }
}

async fn scrape_follows(
    client: &Client,
    token_type: TokenType,
    id: u64,
) -> Result<Result<(HashSet<u64>, HashSet<u64>), UnavailableStatus>, Error> {
    let follower_ids_lookup = client
        .follower_ids(id, token_type)
        .try_collect::<HashSet<_>>();
    let followed_ids_lookup = client
        .followed_ids(id, token_type)
        .try_collect::<HashSet<_>>();

    match futures::try_join!(
        follower_ids_lookup.map_err(Error::from),
        followed_ids_lookup.map_err(Error::from)
    ) {
        Ok(pair) => Ok(Ok(pair)),
        Err(error) => check_unavailable_reason(&client, id, &error)
            .await
            .map_or_else(|| Err(error), |status| Ok(Err(status))),
    }
}

/// Check whether a user token unauthorized error indicates a block, etc.
pub async fn check_unavailable_reason(
    client: &Client,
    id: u64,
    error: &Error,
) -> Option<UnavailableStatus> {
    match error {
        Error::EggMode(error)
            if UnavailableReason::from(error) == UnavailableReason::Unauthorized =>
        {
            match client.lookup_user_or_status(id, TokenType::App).await {
                Ok(user) => Some(UnavailableStatus::Block),
                Err(client_error) => match UnavailableReason::from(&client_error) {
                    UnavailableReason::Suspended => Some(UnavailableStatus::Suspended),
                    UnavailableReason::Deactivated => Some(UnavailableStatus::Deactivated),
                    _ => None,
                },
            }
        }
        _ => None,
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
    Duration::seconds((((count / 75_000) + 1) * 15 * 60) as i64)
        + Duration::seconds(RUN_DURATION_BUFFER_S)
}
