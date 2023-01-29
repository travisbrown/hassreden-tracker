use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use hst_cli::prelude::*;
use hst_tw_profiles_dir::ProfilesDir;
use hst_tw_utils::parse_date_time;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let dir = ProfilesDir::open(opts.data)?;

    match opts.command {
        Command::Extract { start, seen } => {
            let start_date = start.map_or(Ok(None), |date_str| {
                NaiveDate::parse_from_str(&date_str, "%Y-%m-%d").map(Some)
            })?;

            let mut seen_by_id = HashMap::<u64, HashSet<_>>::new();

            for line in BufReader::new(File::open(seen)?).lines() {
                let line = line?;
                let parts = line.split(",").collect::<Vec<_>>();
                let user_id = parts[0].parse::<u64>().unwrap();
                let snapshot = parts[1].parse::<i64>().unwrap();

                let snapshots = seen_by_id.entry(user_id).or_default();
                snapshots.insert(snapshot);
            }

            for value in dir.profiles(start_date, None) {
                let value = value?;

                let (user_id, snapshot) = extract(&value).unwrap();
                if let Some(seen_snapshots) = seen_by_id.get(&user_id) {
                    if !seen_snapshots.contains(&snapshot) {
                        println!("{}", value.to_string());
                    }
                }
            }
        }
        Command::Report { deactivations } => {
            let mut deactivations_by_id = HashMap::new();

            for line in BufReader::new(File::open(deactivations)?).lines() {
                let line = line?;
                let parts = line.split(",").collect::<Vec<_>>();
                let user_id = parts[0].parse::<u64>().unwrap();
                let status_code = parts[1].parse::<u32>().unwrap();

                deactivations_by_id.insert(user_id, status_code);
            }

            let mut by_id = HashMap::<_, ReportEntry>::new();

            for value in dir.profiles(None, None) {
                let value = value?;

                let id = value
                    .get("id_str")
                    .and_then(|id_value| id_value.as_str())
                    .and_then(|id_str| id_str.parse::<u64>().ok())
                    .expect(&format!("Invalid ID: {}", value).to_string());

                let entry = by_id.entry(id).or_default();

                entry
                    .update(&value)
                    .expect(&format!("Invalid value: {}", value).to_string());
            }

            let mut entries = by_id
                .into_iter()
                .map(|(_, entry)| entry)
                .collect::<Vec<_>>();
            entries.sort_by_key(|entry| entry.id);

            for entry in entries {
                let deactivation_status = deactivations_by_id.get(&entry.id).copied();

                println!(
                    "{},{},{},{},{},{},{},{},{},{}",
                    entry.id,
                    deactivation_status
                        .map(|status| status.to_string())
                        .unwrap_or_default(),
                    entry.screen_names.join(";"),
                    entry.created.date_naive(),
                    entry.first_seen.date_naive(),
                    entry
                        .first_seen_without
                        .map(|status| status.date_naive().to_string())
                        .unwrap_or_default(),
                    entry.first_seen_followers_count,
                    entry.current_followers_count,
                    entry.first_seen_verified,
                    entry.current_verified
                );
            }
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "blue", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Data file directory path
    #[clap(long)]
    data: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Extract {
        /// Start date
        #[clap(long)]
        start: Option<String>,
        /// Seen file (two columns: ID, snapshot timestamp)
        #[clap(long)]
        seen: String,
    },
    Report {
        /// Deactivations file
        #[clap(long)]
        deactivations: String,
    },
}

fn extract(value: &serde_json::Value) -> Option<(u64, i64)> {
    let id = value.get("id_str")?.as_str()?.parse::<u64>().ok()?;
    let snapshot = value.get("snapshot")?.as_i64()?;

    Some((id, snapshot))
}

struct ReportEntry {
    id: u64,
    screen_names: Vec<String>,
    created: DateTime<Utc>,
    deactivation_status: Option<u32>,
    first_seen: DateTime<Utc>,
    first_seen_without: Option<DateTime<Utc>>,
    first_seen_followers_count: usize,
    first_seen_verified: bool,
    current_followers_count: usize,
    current_verified: bool,
}

impl Default for ReportEntry {
    fn default() -> ReportEntry {
        ReportEntry {
            id: 0,
            screen_names: vec![],
            created: DateTime::<Utc>::MAX_UTC,
            deactivation_status: None,
            first_seen: DateTime::<Utc>::MAX_UTC,
            first_seen_without: None,
            first_seen_followers_count: 0,
            first_seen_verified: false,
            current_followers_count: 0,
            current_verified: false,
        }
    }
}

impl ReportEntry {
    fn update(&mut self, value: &serde_json::Value) -> Option<()> {
        let id = value.get("id_str")?.as_str()?.parse::<u64>().ok()?;
        let screen_name = value.get("screen_name")?.as_str()?;
        let snapshot = value.get("snapshot")?.as_i64()?;
        let snapshot_timestamp = Utc.timestamp_opt(snapshot, 0).single()?;
        let created = parse_date_time(value.get("created_at")?.as_str()?).ok()?;
        let is_blue_verified = match value.get("ext_is_blue_verified") {
            Some(is_blue_verified) => is_blue_verified.as_bool()?,
            None => false,
        };
        let verified = value.get("verified")?.as_bool()?;
        let followers_count = value.get("followers_count")?.as_u64()? as usize;

        if self.id == 0 {
            self.id = id;
            self.created = created;
        }

        if self.id != id {
            log::error!("Cannot add entries with different IDs");
            None
        } else if self.created != created {
            log::error!("Cannot add entries with different creation dates");
            None
        } else {
            if self
                .screen_names
                .last()
                .filter(|last_screen_name| *last_screen_name == screen_name)
                .is_none()
            {
                self.screen_names.push(screen_name.to_string());
            }

            if self.first_seen == DateTime::<Utc>::MAX_UTC && is_blue_verified {
                self.first_seen = snapshot_timestamp;
                self.first_seen_followers_count = followers_count;
                self.first_seen_verified = verified;
            } else if self.first_seen != DateTime::<Utc>::MAX_UTC
                && self.first_seen_without.is_none()
                && !is_blue_verified
            {
                self.first_seen_without = Some(snapshot_timestamp);
            } else if self.first_seen_without.is_some() && is_blue_verified {
                self.first_seen_without = None;
            }
            self.current_followers_count = followers_count;
            self.current_verified = verified;

            Some(())
        }
    }

    /*fn from_value(value: &serde_json::Value) -> Option<Self> {
        let id = value.get("id_str")?.as_str()?.parse::<u64>().ok()?;
        let screen_name = value.get("screen_name")?.as_str()?;
        let snapshot = value.get("snapshot")?.as_i64()?;
        let snapshot_timestamp = Utc.timestamp_opt(snapshot, 0).single()?;
        let created = parse_date_time(value.get("created_at")?.as_str()?).ok()?;
        let is_blue_verified = match value.get("ext_is_blue_verified") {
            Some(is_blue_verified) => is_blue_verified.as_bool()?,
            None => false,
        };
        let verified = value.get("verified")?.as_bool()?;
        let followers_count = value.get("followers_count")?.as_u64()? as usize;

        Some(ReportEntry {
            id,
            screen_names: vec![screen_name.to_string()],
            created,
            deactivation_status: None,
            first_seen: if is_blue_verified {
                snapshot_timestamp
            } else {
                DateTime::<Utc>::MAX_UTC
            },
            first_seen_without: if is_blue_verified {
                None
            } else {
                Some(snapshot_timestamp)
            },
            first_seen_followers_count: followers_count,
            first_seen_verified: verified,
            current_followers_count: followers_count,
            current_verified: verified,
        })
    }*/
}
