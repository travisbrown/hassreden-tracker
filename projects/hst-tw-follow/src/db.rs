use super::{Batch, Change};
use chrono::{DateTime, TimeZone, Utc};
use futures::TryStreamExt;
use sqlx::{Connection, PgConnection, QueryBuilder};
use std::collections::HashSet;
use std::convert::TryFrom;

#[derive(Clone, Debug)]
pub struct CurrentUserRelations {
    follower_ids: HashSet<u64>,
    followed_ids: HashSet<u64>,
    last_batch_id: i32,
    last_batch_timestamp: DateTime<Utc>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("SQL error")]
    Sqlx(#[from] sqlx::Error),
    #[error("Archive error")]
    Archive(#[from] super::archive::Error),
    #[error("Invalid ID")]
    InvalidId(u64),
    #[error("Invalid timestamp")]
    InvalidTimestamp(DateTime<Utc>),
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

pub async fn update_from_batches<E, I: Iterator<Item = Result<Batch, E>>>(
    connection: &mut PgConnection,
    batches: I,
) -> Result<(), Error>
where
    Error: From<E>,
{
    for batch in batches {
        let batch = batch?;
        println!("batch {}", batch.user_id);
        update_from_batch(connection, &batch, None).await?;
    }

    Ok(())
}

pub async fn update_from_full(
    connection: &mut PgConnection,
    timestamp: DateTime<Utc>,
    user_id: u64,
    follower_ids: HashSet<u64>,
    followed_ids: HashSet<u64>,
) -> Result<(), Error> {
    let user_relations = get_user_relations(connection, user_id).await?;

    let (follower_change, followed_change) = match user_relations {
        Some(ref user_relations) => {
            let follower_change = subtract_old(&user_relations.follower_ids, follower_ids);
            let followed_change = subtract_old(&user_relations.followed_ids, followed_ids);

            (follower_change, followed_change)
        }
        None => {
            let mut follower_addition_ids = follower_ids.into_iter().collect::<Vec<_>>();
            let mut followed_addition_ids = followed_ids.into_iter().collect::<Vec<_>>();

            follower_addition_ids.sort_unstable();
            followed_addition_ids.sort_unstable();

            let follower_change = Change::new(follower_addition_ids, vec![]);
            let followed_change = Change::new(followed_addition_ids, vec![]);

            (follower_change, followed_change)
        }
    };

    let batch = Batch::new(timestamp, user_id, follower_change, followed_change);

    update_from_batch(
        connection,
        &batch,
        Some(user_relations.map(|user_relations| user_relations.last_batch_id)),
    )
    .await
}

pub async fn update_from_batch(
    connection: &mut PgConnection,
    batch: &Batch,
    last_batch_id: Option<Option<i32>>,
) -> Result<(), Error> {
    let mut tx = connection.begin().await?;

    let last_batch_id = match last_batch_id {
        Some(value) => value,
        None => {
            sqlx::query_scalar!(
                "SELECT id from batches WHERE user_id = $1 ORDER BY timestamp DESC LIMIT 1",
                u64_to_i64(batch.user_id)?
            )
            .fetch_optional(&mut tx)
            .await?
        }
    };

    let batch_id = sqlx::query_scalar!(
        "INSERT INTO batches (user_id, timestamp) VALUES ($1, $2) RETURNING id",
        u64_to_i64(batch.user_id)?,
        i32::try_from(batch.timestamp.timestamp())
            .map_err(|_| Error::InvalidTimestamp(batch.timestamp))?
    )
    .fetch_one(&mut tx)
    .await?;

    if let Some(last_batch_id) = last_batch_id {
        sqlx::query_scalar!(
            "UPDATE batches SET next_id = $1 WHERE id = $2 AND next_id IS NULL RETURNING id",
            batch_id,
            last_batch_id,
        )
        .fetch_one(&mut tx)
        .await?;
    }

    for added_follower_id in &batch.follower_change.addition_ids {
        sqlx::query!(
                "INSERT INTO entries (batch_id, user_id, is_follower, is_addition) VALUES ($1, $2, TRUE, TRUE)",
                batch_id,
                u64_to_i64(*added_follower_id)?,
            )
            .execute(&mut tx)
            .await?;
    }

    for removed_follower_id in &batch.follower_change.removal_ids {
        sqlx::query!(
                "INSERT INTO entries (batch_id, user_id, is_follower, is_addition) VALUES ($1, $2, TRUE, FALSE)",
                batch_id,
                u64_to_i64(*removed_follower_id)?,
            )
            .execute(&mut tx)
            .await?;
    }

    for added_followed_id in &batch.followed_change.addition_ids {
        sqlx::query!(
                "INSERT INTO entries (batch_id, user_id, is_follower, is_addition) VALUES ($1, $2, FALSE, TRUE)",
                batch_id,
                u64_to_i64(*added_followed_id)?,
            )
            .execute(&mut tx)
            .await?;
    }

    for removed_followed_id in &batch.followed_change.removal_ids {
        sqlx::query!(
                "INSERT INTO entries (batch_id, user_id, is_follower, is_addition) VALUES ($1, $2, FALSE, FALSE)",
                batch_id,
                u64_to_i64(*removed_followed_id)?,
            )
            .execute(&mut tx)
            .await?;
    }

    tx.commit().await?;

    Ok(())
}

pub async fn get_user_relations(
    connection: &mut PgConnection,
    source_user_id: u64,
) -> Result<Option<CurrentUserRelations>, Error> {
    let results = sqlx::query!(
        r#"SELECT
                batches.id as batch_id,
                batches.timestamp,
                entries.user_id AS "user_id?",
                entries.is_follower AS "is_follower?",
                entries.is_addition AS "is_addition?"
                FROM batches
                LEFT JOIN entries ON entries.batch_id = batches.id
                WHERE batches.user_id = $1
                ORDER BY batches.timestamp"#,
        u64_to_i64(source_user_id)?
    )
    .fetch(connection);

    let (follower_ids, followed_ids, batch_info) = results
        .map_err(Error::from)
        .try_fold(
            (HashSet::new(), HashSet::new(), None),
            |(mut follower_ids, mut followed_ids, _batch_info), row| async move {
                if let Some(((user_id, is_follower), is_addition)) =
                    row.user_id.zip(row.is_follower).zip(row.is_addition)
                {
                    let target_user_id = user_id as u64;

                    if is_follower {
                        if is_addition {
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
                        if is_addition {
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
                }

                Ok((
                    follower_ids,
                    followed_ids,
                    Some((row.batch_id, Utc.timestamp(row.timestamp.into(), 0))),
                ))
            },
        )
        .await?;

    Ok(batch_info.map(
        |(last_batch_id, last_batch_timestamp)| CurrentUserRelations {
            follower_ids,
            followed_ids,
            last_batch_id,
            last_batch_timestamp,
        },
    ))
}

fn u64_to_i64(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::InvalidId(value))
}

fn subtract_old(old_ids: &HashSet<u64>, mut new_ids: HashSet<u64>) -> Change {
    let mut removal_ids = vec![];

    for id in old_ids {
        if !new_ids.remove(id) {
            removal_ids.push(*id);
        }
    }

    let mut addition_ids = new_ids.into_iter().collect::<Vec<_>>();

    addition_ids.sort_unstable();
    removal_ids.sort_unstable();

    Change::new(addition_ids, removal_ids)
}
