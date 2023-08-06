use crate::{
    age::ProfileAgeDb,
    dbs::tracked::{TrackedUser, TrackedUserDb},
    store::Store,
    Batch,
};
use chrono::{Duration, SubsecRound, Utc};
use egg_mode_extras::{
    client::{Client, FormerUserStatus, TokenType},
    error::UnavailableReason,
};
use futures::{future::TryFutureExt, stream::TryStreamExt};
use hst_deactivations::file::DeactivationFile;
use hst_tw_db::{table::ReadOnly, ProfileDb};
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

const RUN_DURATION_BUFFER_S: i64 = 20 * 60;

const MIN_FOLLOWERS_COUNT: usize = 15_000;
const MAX_FOLLOWERS_COUNT: usize = 1_000_000;
const MIN_TARGET_AGE_H: i64 = 24;
const MAX_TARGET_AGE_D: i64 = 1;

/// This is supposed to be 15 minutes but in practice seems longer.
const RATE_LIMIT_WINDOW_S: i64 = 24 * 60;
const RATE_LIMIT_WINDOW_BATCH_SIZE: usize = 75_000;

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
    #[error("Profile database error")]
    ProfileDb(#[from] hst_tw_db::Error),
    #[error("Profile age database error")]
    ProfileAgeDb(#[from] crate::age::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnavailableStatus {
    Block,
    Deactivated,
    Suspended,
    Protected,
    Unknown,
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
    twitter_client: Arc<Client>,
    downloader_client: Arc<Client>,
    store: Store,
    tracked: TrackedUserDb,
    deactivations: DeactivationFile,
    failed: HashSet<u64>,
    pub profile_age_db: ProfileAgeDb,
}

impl Session {
    pub fn open<P: AsRef<Path>>(
        twitter_client: Client,
        downloader_client: Client,
        store_path: P,
        tracked_path: P,
        deactivations_path: P,
        profile_age_db_path: P,
    ) -> Result<Self, Error> {
        let store = Store::open(store_path)?;
        let tracked = TrackedUserDb::open(tracked_path)?;
        let deactivations = DeactivationFile::open(deactivations_path)?;
        let profile_age_db = ProfileAgeDb::open(profile_age_db_path, false)?;

        Ok(Self {
            twitter_client: Arc::new(twitter_client),
            downloader_client: Arc::new(downloader_client),
            store,
            tracked,
            deactivations,
            failed: HashSet::new(),
            profile_age_db,
        })
    }

    pub fn downloader<P: AsRef<Path>>(&self, path: P) -> super::downloader::Downloader {
        super::downloader::Downloader::new(
            path.as_ref().to_path_buf().into_boxed_path(),
            self.downloader_client.clone(),
            self.deactivations.clone(),
            self.profile_age_db.clone(),
            default_profile_target_age(),
        )
    }

    pub async fn run(&mut self, token_type: TokenType) -> Result<Option<RunInfo>, Error> {
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

            let mut new_users = tracked_users.clone();

            for (id, _, _) in &user_updates {
                new_users.remove(id);
            }

            let mut candidates = vec![];
            let now = Utc::now().trunc_subsecs(0);

            for (id, user) in new_users {
                candidates.push((id, Some(user), Duration::max_value()));
            }

            for (id, last_update, available) in user_updates {
                if available && self.deactivations.status(id).is_none() {
                    let user = tracked_users.remove(&id);

                    if !user
                        .as_ref()
                        .map(|user| {
                            user.protected || check_block(&self.downloader_client, user, token_type)
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

                        if !self.failed.contains(&id) {
                            candidates.push((id, user, age - target_age));
                        }
                    }
                }
            }

            match candidates
                .into_iter()
                .max_by_key(|(id, _, diff)| (*diff, Reverse(*id)))
            {
                Some((id, user, _)) => {
                    let res = self.scrape(token_type, id, user).await;
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    res
                }
                None => Ok(None),
            }
        }
    }

    /// Request downloads for batch.
    fn enqueue_batch(&self, batch: &Batch) -> Result<usize, Error> {
        let mut ids = HashSet::new();

        if let Some(change) = &batch.follower_change {
            for id in &change.addition_ids {
                ids.insert(*id);
            }

            for id in &change.removal_ids {
                if self.deactivations.status(*id).is_none() {
                    ids.insert(*id);
                }
            }
        }

        if let Some(change) = &batch.followed_change {
            for id in &change.addition_ids {
                ids.insert(*id);
            }

            for id in &change.removal_ids {
                if self.deactivations.status(*id).is_none() {
                    ids.insert(*id);
                }
            }
        }

        let len = ids.len();

        for id in ids {
            self.profile_age_db
                .prioritize(id, default_profile_target_age())?;
        }

        Ok(len)
    }

    pub async fn scrape(
        &mut self,
        token_type: TokenType,
        user_id: u64,
        user: Option<TrackedUser>,
    ) -> Result<Option<RunInfo>, Error> {
        let user = match user {
            Some(user) => Some(user),
            None => self.tracked.get(user_id)?,
        };

        let tag = if token_type == TokenType::App {
            "[APPL]"
        } else {
            "[USER]"
        };
        hst_cli::prelude::log::info!("{} RUNNING: {}", tag, user_id);

        if let Some(last_update) = self.store.check_out(
            user_id,
            estimate_run_duration(user.map(|user| user.followers_count).unwrap_or(15_000)),
        )? {
            match scrape_follows(
                &self.twitter_client,
                &self.downloader_client,
                token_type,
                user_id,
            )
            .await?
            {
                Ok((follower_ids, followed_ids)) => {
                    let batch = self.store.make_batch(user_id, follower_ids, followed_ids);
                    self.store.update_and_write(&batch, last_update)?;
                    self.enqueue_batch(&batch)?;

                    Ok(Some(RunInfo::Scraped { batch }))
                }
                Err(status) => {
                    match status {
                        UnavailableStatus::Block => {
                            self.tracked
                                .put_block(user_id, self.downloader_client.user_id())?;
                        }
                        UnavailableStatus::Deactivated => {
                            self.deactivations
                                .add(user_id, 50, Utc::now().trunc_subsecs(0));
                            self.deactivations.flush()?;
                        }
                        UnavailableStatus::Suspended => {
                            self.deactivations
                                .add(user_id, 63, Utc::now().trunc_subsecs(0));
                            self.deactivations.flush()?;
                        }
                        UnavailableStatus::Protected => {
                            self.tracked.set_protected(user_id, true)?;
                        }
                        UnavailableStatus::Unknown => {}
                    }
                    self.failed.insert(user_id);

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

    pub fn compare_users(&self) -> Result<(Vec<u64>, Vec<u64>), Error> {
        let store_ids = self.store.user_ids().into_iter().collect::<HashSet<_>>();
        let tracked_db_ids = self
            .tracked
            .users()?
            .into_iter()
            .map(|user| user.id)
            .collect::<HashSet<_>>();

        let mut store_only_ids = store_ids
            .difference(&tracked_db_ids)
            .copied()
            .collect::<Vec<_>>();
        let mut tracked_db_only_ids = tracked_db_ids
            .difference(&store_ids)
            .copied()
            .collect::<Vec<_>>();

        store_only_ids.sort_unstable();
        tracked_db_only_ids.sort_unstable();

        Ok((store_only_ids, tracked_db_only_ids))
    }

    pub fn reload_profile_ages(&self, profile_db: &ProfileDb<ReadOnly>) -> Result<(), Error> {
        let mut ranks = self.store.user_ranks()?;
        let mut count = 0;

        for result in profile_db.user_id_iter() {
            let (id, _, last) = result?;
            let rank = ranks.remove(&id).unwrap_or(10_000_000);

            if self.deactivations.status(id).is_none() {
                let target_age = profile_target_age(rank);

                self.profile_age_db.insert(id, Some(last), target_age)?;

                count += 1;

                if count % 100000 == 0 {
                    println!("{}", count);
                }
            }
        }

        for (id, rank) in ranks {
            let target_age = profile_target_age(rank);

            self.profile_age_db.insert(id, None, target_age)?;
        }

        Ok(())
    }

    /// Remove deactivated users from the profile age database.
    pub fn clean_profile_ages(&self) -> Result<usize, Error> {
        let ids = self.deactivations.log().current_deactivated(None);
        let mut count = 0;

        for id in ids {
            if self.profile_age_db.delete(id)? {
                count += 1;
            }
        }

        Ok(count)
    }
}

async fn scrape_follows(
    client: &Client,
    downloader_client: &Client,
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
        Err(error) => check_unavailable_reason(client, downloader_client, id, &error)
            .await
            .map_or_else(|| Err(error), |status| Ok(Err(status))),
    }
}

/// Check whether a user token unauthorized error indicates a block, etc.
pub async fn check_unavailable_reason(
    client: &Client,
    downloader_client: &Client,
    id: u64,
    error: &Error,
) -> Option<UnavailableStatus> {
    Some(UnavailableStatus::Unknown)
    /*match error {
        Error::EggMode(error) => {
            let reason = UnavailableReason::from(error);
            if reason == UnavailableReason::Unauthorized
                || reason == UnavailableReason::DoesNotExist
            {
                downloader_client
                    .lookup_user_or_status(id, TokenType::App)
                    .await
                    .ok()
                    .map(|result| match result {
                        Ok(profile) if profile.protected => UnavailableStatus::Protected,
                        Ok(_) => UnavailableStatus::Block,
                        Err(FormerUserStatus::Deactivated) => UnavailableStatus::Deactivated,
                        Err(FormerUserStatus::Suspended) => UnavailableStatus::Suspended,
                    })
            } else {
                None
            }
        }
        _ => None,
    }*/
}

fn check_block(client: &Client, user: &TrackedUser, token_type: TokenType) -> bool {
    token_type == TokenType::User && user.blocks.contains(&client.user_id())
}

fn default_target_age(followers_count: usize) -> Duration {
    let min_count = MIN_FOLLOWERS_COUNT;
    let max_count = MAX_FOLLOWERS_COUNT;
    let count_range = max_count - min_count;
    let min_target_age = Duration::hours(MIN_TARGET_AGE_H);
    let max_target_age = Duration::days(MAX_TARGET_AGE_D);
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
    Duration::seconds(((count / RATE_LIMIT_WINDOW_BATCH_SIZE) + 1) as i64 * RATE_LIMIT_WINDOW_S)
        + Duration::seconds(RUN_DURATION_BUFFER_S)
}

fn default_profile_target_age() -> Duration {
    Duration::days(7)
}

fn profile_target_age(rank: usize) -> Duration {
    if rank <= 100_000 {
        Duration::hours(6)
    } else if rank <= 1_000_000 {
        Duration::days(1)
    } else if rank <= 10_000_000 {
        Duration::days(7)
    } else {
        Duration::days(30)
    }
}
