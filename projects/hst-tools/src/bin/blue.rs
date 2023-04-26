use chrono::{TimeZone, Utc};
use hst_cli::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    Subscribed,
    Unsubscribed,
    Suspended,
    Deactivated,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Subscribed => "B",
            Self::Unsubscribed => "U",
            Self::Suspended => "S",
            Self::Deactivated => "D",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Verified {
    Business,
    Government,
    Unknown,
}

impl Verified {
    fn from_fields(is_verified: bool, verified_type: Option<&str>) -> Option<Option<Self>> {
        match verified_type {
            Some("Business") => Some(Some(Self::Business)),
            Some("Government") => Some(Some(Self::Government)),
            Some(_) => None,
            None => None,
        }
    }
}

impl std::fmt::Display for Verified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Business => "B",
            Self::Government => "G",
            Self::Unknown => "V",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Record {
    id: u64,
    screen_name: String,
    snapshot: i64,
    first_seen_blue: Option<i64>,
    followers_count: usize,
    verified: Option<Verified>,
    status: Status,
}

impl Record {
    fn from_json(value: &Value) -> Option<Self> {
        let id = value.get("id_str")?.as_str()?.parse::<u64>().ok()?;
        let screen_name = value.get("screen_name")?.as_str()?.to_string();
        let snapshot = value.get("snapshot")?.as_i64()?;
        let followers_count = value.get("followers_count")?.as_u64()? as usize;
        let is_verified = value.get("verified")?.as_bool()?;
        let is_blue_verified = value.get("ext_is_blue_verified")?.as_bool()?;
        let verified_type = value
            .get("ext_verified_type")
            .and_then(|verified_type_value| verified_type_value.as_str());

        let first_seen_blue = if is_blue_verified {
            Some(snapshot)
        } else {
            None
        };

        let verified = Verified::from_fields(is_verified, verified_type)?;

        let status = if is_blue_verified {
            Status::Subscribed
        } else {
            Status::Unsubscribed
        };

        Some(Record {
            id,
            screen_name,
            snapshot,
            first_seen_blue,
            followers_count,
            verified,
            status,
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let mut seen = HashSet::new();
    let mut suspended = HashSet::new();
    let mut deactivated = HashSet::new();
    let mut records = HashMap::new();

    let reader = BufReader::new(File::open(opts.current)?);

    for line in reader.lines() {
        let line = line?;
        if line.starts_with('{') {
            let json = serde_json::from_str(&line)?;
            let record =
                Record::from_json(&json).unwrap_or_else(|| panic!("Invalid line {}", line));
            if !seen.contains(&record.id) {
                seen.insert(record.id);
                records.insert(record.id, record);
            }
        } else {
            let parts = line.split(',').collect::<Vec<_>>();
            let id = parts[0].parse::<u64>().unwrap();
            if !seen.contains(&id) {
                seen.insert(id);
                if parts[1] == "63" {
                    suspended.insert(id);
                } else if parts[1] == "50" {
                    deactivated.insert(id);
                } else {
                    panic!("Unexpected deactivation status: {}", line);
                }
            }
        }
    }

    log::info!(
        "Current read: {} found, {} suspended, {} deactivated",
        records.len(),
        suspended.len(),
        deactivated.len()
    );

    let reader = BufReader::new(File::open(opts.data)?);

    for line in reader.lines() {
        let line = line?;
        let json = serde_json::from_str(&line)?;
        if let Some(mut current) = Record::from_json(&json) {
            match records.get_mut(&current.id) {
                Some(previous) => {
                    let previous_first_seen_blue = previous.first_seen_blue;
                    let current_first_seen_blue = current.first_seen_blue;

                    let new_first_seen_blue =
                        match (previous_first_seen_blue, current_first_seen_blue) {
                            (Some(previous), Some(current)) => Some(previous.min(current)),
                            (Some(previous), None) => Some(previous),
                            (None, Some(current)) => Some(current),
                            (None, None) => None,
                        };

                    if (previous.status == Status::Suspended
                        || previous.status == Status::Deactivated)
                        && current.snapshot > previous.snapshot
                    {
                        current.status = previous.status;
                        *previous = current;
                    }

                    previous.first_seen_blue = new_first_seen_blue;
                }
                None => {
                    if suspended.contains(&current.id) {
                        current.status = Status::Suspended;
                        records.insert(current.id, current);
                    } else if deactivated.contains(&current.id) {
                        current.status = Status::Deactivated;
                        records.insert(current.id, current);
                    } else {
                        log::error!("Unknown profile ID: {}", line);
                    }
                }
            }
        } else {
            log::error!("Invalid record: {}", json);
        }
    }

    let mut records = records.into_iter().collect::<Vec<_>>();
    records.sort_by_key(|(id, _)| *id);

    for (_, record) in records {
        println!(
            "{},{},{},{},{},{},{}",
            record.id,
            record.screen_name,
            record
                .verified
                .map(|verified| verified.to_string())
                .unwrap_or_default(),
            record.followers_count,
            Utc.timestamp_opt(record.first_seen_blue.unwrap(), 0)
                .unwrap()
                .date_naive(),
            record.first_seen_blue.unwrap(),
            record.status
        );
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "blue", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Data file path
    #[clap(long)]
    data: String,
    /// Current file path
    #[clap(long)]
    current: String,
}
