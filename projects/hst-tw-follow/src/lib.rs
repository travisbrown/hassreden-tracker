use chrono::{DateTime, TimeZone, Utc};
use futures::TryStreamExt;
use sqlx::{Connection, PgConnection};
use std::collections::HashSet;
use std::convert::TryFrom;

#[derive(Clone, Debug)]
pub struct CurrentUserRelations {
    follower_ids: HashSet<u64>,
    followed_ids: HashSet<u64>,
    last_update_id: i32,
    last_update_timestamp: DateTime<Utc>,
}

struct Update {
    added_ids: Vec<u64>,
    removed_ids: Vec<u64>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("SQL error")]
    Sqlx(#[from] sqlx::Error),
    #[error("Invalid ID")]
    InvalidId(u64),
    #[error("Removing missing ID")]
    InvalidRemoval {
        source_user_id: u64,
        target_user_id: u64,
    },
    #[error("Duplicate ID")]
    InvalidAddition {
        source_user_id: u64,
        target_user_id: u64,
    },
}

pub async fn update_user_relations(
    connection: &mut PgConnection,
    user_id: u64,
    follower_ids: HashSet<u64>,
    followed_ids: HashSet<u64>,
) -> Result<(), Error> {
    if let Some(current_user_info) = get_user_relations(connection, user_id).await? {
        let follower_update = subtract_old(current_user_info.follower_ids, follower_ids);
        let followed_update = subtract_old(current_user_info.followed_ids, followed_ids);

        let tx = connection.begin().await?;
    }

    Ok(())
}

pub async fn get_user_relations(
    connection: &mut PgConnection,
    source_user_id: u64,
) -> Result<Option<CurrentUserRelations>, Error> {
    let results = sqlx::query!(
            "SELECT updates.id as update_id, updates.timestamp, entries.user_id, entries.follow, entries.addition
                FROM updates
                JOIN entries ON entries.update_id = updates.id
                WHERE updates.user_id = $1
                ORDER BY updates.timestamp",
                u64_to_i64(source_user_id)?)
            .fetch(connection);

    let (follower_ids, followed_ids, user_info) = results
        .map_err(Error::from)
        .try_fold(
            (HashSet::new(), HashSet::new(), None),
            |(mut follower_ids, mut followed_ids, update_info), row| async move {
                let target_user_id = row.user_id as u64;

                if !row.follow {
                    if row.addition {
                        if follower_ids.insert(target_user_id) {
                            Ok(())
                        } else {
                            Err(Error::InvalidAddition {
                                source_user_id,
                                target_user_id,
                            })
                        }
                    } else {
                        if follower_ids.remove(&target_user_id) {
                            Ok(())
                        } else {
                            Err(Error::InvalidRemoval {
                                source_user_id,
                                target_user_id,
                            })
                        }
                    }
                } else {
                    if row.addition {
                        if followed_ids.insert(target_user_id) {
                            Ok(())
                        } else {
                            Err(Error::InvalidAddition {
                                source_user_id,
                                target_user_id,
                            })
                        }
                    } else {
                        if followed_ids.remove(&target_user_id) {
                            Ok(())
                        } else {
                            Err(Error::InvalidRemoval {
                                source_user_id,
                                target_user_id,
                            })
                        }
                    }
                }?;

                Ok((
                    follower_ids,
                    followed_ids,
                    Some((row.update_id, Utc.timestamp(row.timestamp.into(), 0))),
                ))
            },
        )
        .await?;

    Ok(user_info.map(
        |(last_update_id, last_update_timestamp)| CurrentUserRelations {
            follower_ids,
            followed_ids,
            last_update_id,
            last_update_timestamp,
        },
    ))
}

fn u64_to_i64(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::InvalidId(value))
}

fn subtract_old(old_ids: HashSet<u64>, mut new_ids: HashSet<u64>) -> Update {
    let mut removed_ids = vec![];

    for id in old_ids {
        if !new_ids.remove(&id) {
            removed_ids.push(id);
        }
    }

    let mut added_ids = new_ids.into_iter().collect::<Vec<_>>();

    added_ids.sort_unstable();
    removed_ids.sort_unstable();

    Update {
        added_ids,
        removed_ids,
    }
}
