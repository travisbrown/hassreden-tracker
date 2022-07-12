use super::model::User;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

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

#[derive(Debug, Default, Eq, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct PartialUser {
    pub id: u64,
    pub screen_name: String,
    pub name: Option<String>,
}

impl PartialUser {
    pub fn new(id: u64, screen_name: String, name: Option<String>) -> Self {
        Self {
            id,
            screen_name,
            name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserInfo {
    pub snapshot: DateTime<Utc>,
    pub users: Vec<User>,
    pub partial_users: Vec<PartialUser>,
}

pub fn extract_user_info(
    value: &Value,
    created_at_fallback: bool,
) -> Result<Option<UserInfo>, Error> {
    if value.get("delete").is_none() {
        // We try to determine the snapshot timestamp by checking for a `timestamp_ms` field,
        // and then (if specified) by parsing `created_at` (since `timestamp_ms` isn't available
        // for older Twitter API responses).
        let snapshot = get_timestamp_ms(value)
            .or_else(|| {
                if created_at_fallback {
                    get_created_at(value)
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::MissingTimestamp(value.clone()))?;

        let mut partial_user_map = HashMap::new();

        let user = get_user(value, snapshot)?;
        add_partial_users(value, &mut partial_user_map);

        let mut seen = HashSet::new();
        seen.insert(user.id);

        let mut users = vec![user];

        if let Some(status_value) = value.get("retweeted_status") {
            let user = get_user(status_value, snapshot)?;
            add_partial_users(status_value, &mut partial_user_map);

            if !seen.contains(&user.id) {
                seen.insert(user.id);
                users.push(user);
            }
        }

        if let Some(status_value) = value.get("quoted_status") {
            let user = get_user(status_value, snapshot)?;
            add_partial_users(status_value, &mut partial_user_map);

            if !seen.contains(&user.id) {
                seen.insert(user.id);
                users.push(user);
            }
        }

        let partial_users = partial_user_map
            .into_iter()
            .filter_map(|(id, partial_user)| {
                if seen.contains(&(id as i64)) {
                    None
                } else {
                    Some(partial_user)
                }
            })
            .collect();

        Ok(Some(UserInfo {
            snapshot,
            users,
            partial_users,
        }))
    } else {
        Ok(None)
    }
}

fn add_partial_users(status_value: &Value, acc: &mut HashMap<u64, PartialUser>) {
    for partial_user in get_all_user_mentions(status_value) {
        acc.insert(partial_user.id, partial_user);
    }

    if let Some(partial_user) = get_in_reply_to(status_value) {
        acc.entry(partial_user.id).or_insert(partial_user);
    }
}

fn get_in_reply_to(status_value: &Value) -> Option<PartialUser> {
    let id_str_value = status_value.get("in_reply_to_user_id_str")?;
    let id_str_string = id_str_value.as_str()?;
    let id_str_u64 = id_str_string.parse::<u64>().ok()?;
    let screen_name_value = status_value.get("in_reply_to_screen_name")?;
    let screen_name_string = screen_name_value.as_str()?;
    Some(PartialUser::new(
        id_str_u64,
        screen_name_string.to_string(),
        None,
    ))
}

fn get_all_user_mentions(status_value: &Value) -> Vec<PartialUser> {
    let mut results = get_user_mentions(status_value);

    if let Some(extended_tweet) = status_value.get("extended_tweet") {
        results.extend(get_user_mentions(extended_tweet));
    }

    results
}

fn get_user_mentions(status_value: &Value) -> Vec<PartialUser> {
    status_value
        .get("entities")
        .and_then(|entities| {
            entities
                .get("user_mentions")
                .and_then(|user_mentions| user_mentions.as_array())
        })
        .map(|user_mentions| {
            user_mentions
                .iter()
                .filter_map(|user_mention| {
                    let id_str_value = user_mention.get("id_str")?;
                    let id_str_string = id_str_value.as_str()?;
                    let id_str_u64 = id_str_string.parse::<u64>().ok()?;
                    let screen_name_value = user_mention.get("screen_name")?;
                    let screen_name_string = screen_name_value.as_str()?;
                    let name_value = user_mention.get("name")?;
                    let name_string = name_value.as_str()?;

                    Some(PartialUser::new(
                        id_str_u64,
                        screen_name_string.to_string(),
                        Some(name_string.to_string()),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
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
