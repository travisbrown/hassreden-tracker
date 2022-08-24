use crate::age::ProfileAgeDb;
use chrono::{DateTime, Duration, Utc};
use egg_mode::user::UserID;
use egg_mode_extras::client::{Client, TokenType};
use futures::stream::TryStreamExt;
use hst_deactivations::file::DeactivationFile;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

const MIN_AGE_S: i64 = 120 * 60;
const MIN_RUNNING_S: i64 = 60 * 60;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Twitter API error")]
    EggMode(#[from] egg_mode::error::Error),
    #[error("Twitter API client error")]
    EggModeExtras(#[from] egg_mode_extras::error::Error),
    #[error("Deactivations file error")]
    Deactivations(#[from] hst_deactivations::Error),
    #[error("Profile age database error")]
    ProfileAgeDb(#[from] crate::age::Error),
    #[error("Timestamp field collision")]
    TimestampFieldCollision(Value),
    #[error("Invalid profile JSON")]
    InvalidProfileJson(Value),
    #[error("Unexpected user ID")]
    UnexpectedUserId(UserID),
}

pub struct Downloader {
    base: Box<Path>,
    twitter_client: Arc<Client>,
    deactivations: DeactivationFile,
    profile_age_db: ProfileAgeDb,
}

impl Downloader {
    pub fn new(
        base: Box<Path>,
        twitter_client: Arc<Client>,
        deactivations: DeactivationFile,
        profile_age_db: ProfileAgeDb,
    ) -> Self {
        Self {
            base,
            twitter_client,
            deactivations,
            profile_age_db,
        }
    }

    pub async fn run_batch(&self, count: usize) -> Result<(usize, usize), Error> {
        let ids = self.profile_age_db.get_next(
            count,
            Duration::seconds(MIN_AGE_S),
            Duration::seconds(MIN_RUNNING_S),
        )?;

        hst_cli::prelude::log::info!("Downloading {} (of {})", ids.len(), count);

        let results = self
            .twitter_client
            .lookup_users_json_or_status(ids.into_iter(), TokenType::User)
            .map_err(Error::from)
            .try_collect::<Vec<_>>()
            .await?;

        let mut profiles = Vec::with_capacity(count);
        let mut deactivations = HashMap::new();

        for result in results {
            let now = Utc::now();
            match result {
                Ok(mut value) => {
                    timestamp_json(&mut value, now)?;
                    let id = extract_user_id(&value)?;

                    profiles.push((now, id, value));
                }
                Err((UserID::ID(user_id), status)) => {
                    let status_code = status.code() as u32;
                    deactivations.insert(user_id, (status_code, now));
                }
                Err((user_id, _)) => {
                    return Err(Error::UnexpectedUserId(user_id));
                }
            }
        }

        let deactivations_len = deactivations.len();
        let profiles_len = profiles.len();

        if deactivations_len > 0 {
            self.deactivations.add_all(deactivations);
            self.deactivations.flush()?;
        }

        profiles.sort_by_key(|(timestamp, id, _)| (*timestamp, *id));

        if profiles_len > 0 {
            let timestamp_ms = Utc::now().timestamp_millis();
            let file = File::create(self.base.join(format!("{}.ndjson", timestamp_ms)))?;
            let mut writer = BufWriter::new(file);

            for (_, _, profile) in profiles {
                writeln!(writer, "{}", profile)?;
            }
        }

        Ok((deactivations_len, profiles_len))
    }
}

fn timestamp_json(value: &mut Value, now: DateTime<Utc>) -> Result<(), Error> {
    if let Some(fields) = value.as_object_mut() {
        if let Some(previous_value) =
            fields.insert("snapshot".to_string(), serde_json::json!(now.timestamp()))
        {
            Err(Error::TimestampFieldCollision(previous_value))
        } else {
            Ok(())
        }
    } else {
        Err(Error::InvalidProfileJson(value.clone()))
    }
}

fn extract_user_id(value: &Value) -> Result<u64, Error> {
    value
        .get("id_str")
        .and_then(|id_str_value| id_str_value.as_str())
        .and_then(|id_str| id_str.parse::<u64>().ok())
        .ok_or_else(|| Error::InvalidProfileJson(value.clone()))
}
