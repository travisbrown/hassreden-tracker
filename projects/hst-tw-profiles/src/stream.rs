use super::model::User;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use std::collections::HashSet;

const TIMESTAMP_FIELD_NAME: &str = "snapshot";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Missing snapshot timestamp")]
    MissingTimestamp(Value),
    #[error("Missing user")]
    MissingUser(Value),
    #[error("Invalid user object")]
    InvalidUser(serde_json::error::Error),
}

pub fn extract_user_objects(value: &Value) -> Result<Vec<User>, Error> {
    if value.get("delete").is_none() {
        // We try to determine the snapshot timestamp by checking for a `timestamp_ms` field,
        // and then by parsing `created_at` (since `timestamp_ms` isn't available for older
        // Twitter API responses).
        let snapshot = get_timestamp_ms(value)
            .or_else(|| get_created_at(value))
            .ok_or_else(|| Error::MissingTimestamp(value.clone()))?;

        let user = get_user(value, snapshot)?;

        let mut seen = HashSet::new();
        seen.insert(user.id);

        let mut users = vec![user];

        if let Some(status) = value.get("retweeted_status") {
            let user = get_user(status, snapshot)?;

            if !seen.contains(&user.id) {
                seen.insert(user.id);
                users.push(user);
            }
        }

        if let Some(status) = value.get("quoted_status") {
            let user = get_user(status, snapshot)?;

            if !seen.contains(&user.id) {
                seen.insert(user.id);
                users.push(user);
            }
        }

        Ok(users)
    } else {
        Ok(vec![])
    }
}

fn get_timestamp_ms(value: &Value) -> Option<DateTime<Utc>> {
    let timestamp_ms_value = value.get("timestamp_ms")?;
    let timestamp_ms_string = timestamp_ms_value.as_str()?;
    let timestamp_ms_i64 = timestamp_ms_string.parse::<i64>().ok()?;
    Utc.timestamp_millis_opt(timestamp_ms_i64).single()
}

fn get_created_at(value: &Value) -> Option<DateTime<Utc>> {
    let created_at_value = value.get("created_at")?;
    let created_at_string = created_at_value.as_str()?;
    hst_tw_utils::parse_date_time(created_at_string).ok()
}

fn get_user(value: &Value, snapshot: DateTime<Utc>) -> Result<User, Error> {
    let mut user_value = value
        .get("user")
        .ok_or_else(|| Error::MissingUser(value.clone()))?
        .clone();

    if let Some(fields) = user_value.as_object_mut() {
        fields.insert(
            TIMESTAMP_FIELD_NAME.to_string(),
            serde_json::json!(snapshot.timestamp()),
        );
    }

    serde_json::from_value(user_value).map_err(Error::InvalidUser)
}
