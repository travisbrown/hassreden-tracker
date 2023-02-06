use chrono::{DateTime, TimeZone, Utc};
use egg_mode_extras::{client::TokenType, Client};
use futures::{StreamExt, TryStreamExt};
use hst_cli::prelude::*;
use hst_deactivations::DeactivationLog;
use hst_tw_db::{table::ReadOnly, ProfileDb};
use hst_utils::dedup_unsorted;
use itertools::Itertools;
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

const WAIT_BUFFER_S: u64 = 30;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;
    let client = Client::from_config_file(opts.keys).await?;
    let token_type = if opts.user_token {
        egg_mode_extras::client::TokenType::User
    } else {
        egg_mode_extras::client::TokenType::App
    };

    match opts.command {
        Command::Users => {
            let mut user_ids = std::io::stdin()
                .lock()
                .lines()
                .map(|line| {
                    line.map_err(Error::from).and_then(|line| {
                        line.parse::<u64>()
                            .map_err(|_| Error::InvalidUserIdLine(line))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let removed_count = dedup_unsorted(&mut user_ids);

            if removed_count > 0 {
                log::info!("Removed {removed_count} duplicate IDs");
            }

            client
                .lookup_users_json_or_status(user_ids.iter().copied(), token_type)
                .filter_map(|result| async {
                    match result {
                        Err(egg_mode::error::Error::RateLimit(timestamp_s)) => {
                            let now_s = Utc::now().timestamp() as u64;
                            let diff_s = timestamp_s as u64 - now_s + WAIT_BUFFER_S;

                            if diff_s > 0 {
                                log::warn!("Waiting {} seconds after rate limit error", diff_s);
                                tokio::time::sleep(tokio::time::Duration::from_secs(diff_s)).await;
                            }

                            None
                        }
                        other => Some(other),
                    }
                })
                .map_err(Error::from)
                .try_for_each(|result| async {
                    match result {
                        Ok(mut value) => {
                            timestamp_json(&mut value, &opts.timestamp)?;
                            println!("{}", value);
                        }
                        Err((egg_mode::user::UserID::ID(user_id), status)) => {
                            let timestamp = Utc::now().timestamp();
                            let status_code = status.code();
                            println!("{},{},{},", user_id, status_code, timestamp);
                        }
                        _ => {}
                    }
                    Ok(())
                })
                .await?;
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "scraper", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// TOML file containing Twitter API keys
    #[clap(long, default_value = "keys.toml")]
    keys: String,
    /// Use user token instead of app token
    #[clap(long)]
    user_token: bool,
    /// Timestamp field name to add
    #[clap(long, default_value = "snapshot")]
    timestamp: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Users,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
    #[error("Invalid user ID line")]
    InvalidUserIdLine(String),
    #[error("Expected JSON object")]
    ExpectedJsonObject(Value),
    #[error("Timestamp field collision")]
    TimestampFieldCollision(Value, String),
    #[error("egg-mode error")]
    EggMode(#[from] egg_mode::error::Error),
    #[error("egg-mode-extras error")]
    EggModeExtras(#[from] egg_mode_extras::error::Error),
}

fn timestamp_json(value: &mut Value, key: &str) -> Result<(), Error> {
    if let Some(fields) = value.as_object_mut() {
        if let Some(_) = fields.insert(key.to_string(), serde_json::json!(Utc::now().timestamp())) {
            return Err(Error::TimestampFieldCollision(
                value.clone(),
                key.to_string(),
            ));
        }
        Ok(())
    } else {
        Err(Error::ExpectedJsonObject(value.clone()))
    }
}

async fn process_batch(
    client: &Client,
    user_ids: &[u64],
    token_type: TokenType,
    timestamp: &str,
) -> Result<Vec<u64>, Error> {
    //let mut succeeded_ids = HashSet::new();

    client
        .lookup_users_json_or_status(user_ids.iter().copied(), token_type)
        .filter_map(|result| async {
            match result {
                Err(error) => {
                    if continue_after_error(&error).await {
                        None
                    } else {
                        Some(Err(error))
                    }
                }
                other => Some(other),
            }
        })
        .map_err(Error::from)
        .try_for_each(|result| async {
            match result {
                Ok(mut value) => {
                    timestamp_json(&mut value, &timestamp)?;
                    println!("{}", value);
                }
                Err((egg_mode::user::UserID::ID(user_id), status)) => {
                    let timestamp = Utc::now().timestamp();
                    let status_code = status.code();
                    println!("{},{},{},", user_id, status_code, timestamp);
                }
                _ => {}
            }
            Ok(())
        })
        .await?;

    Ok(vec![])
}

async fn continue_after_error(error: &egg_mode::error::Error) -> bool {
    match error {
        egg_mode::error::Error::RateLimit(timestamp_s) => {
            let now_s = Utc::now().timestamp() as u64;
            let diff_s = *timestamp_s as u64 - now_s + WAIT_BUFFER_S;

            if diff_s > 0 {
                log::warn!("Waiting {} seconds after rate limit error", diff_s);
                tokio::time::sleep(tokio::time::Duration::from_secs(diff_s)).await;
            }

            true
        }
        _ => false,
    }
}
