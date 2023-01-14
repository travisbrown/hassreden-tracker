use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use hst_cli::prelude::*;
use hst_tw_profiles_dir::ProfilesDir;
use hst_tw_utils::parse_date_time;
use regex::Regex;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let exclusions = match opts.exclusions {
        Some(exclusions) => {
            let reader = BufReader::new(File::open(&opts.data)?);
            let mut ids = HashSet::new();
            let reader = BufReader::new(File::open(exclusions)?);

            for line in reader.lines() {
                let line = line?;
                let id = line.parse::<u64>()?;
                ids.insert(id);
            }

            ids
        }
        None => HashSet::new(),
    };

    let reader = BufReader::new(File::open(opts.data)?);
    let mut by_id = HashMap::<u64, (i64, u64, serde_json::Value)>::new();

    for line in reader.lines() {
        let line = line?;
        let mut value: serde_json::Value = serde_json::from_str(&line)?;

        let id = value
            .get("id_str")
            .and_then(|id_value| id_value.as_str())
            .and_then(|id_str| id_str.parse::<u64>().ok())
            .expect(&format!("Invalid ID: {}", value).to_string());

        if !exclusions.contains(&id) {
            let snapshot = value
                .get("snapshot")
                .and_then(|snapshot_value| snapshot_value.as_i64())
                .expect(&format!("Invalid snapshot: {}", value).to_string());

            let snapshot_utc = Utc.timestamp_opt(snapshot, 0).unwrap();

            if let Some(fields) = value.as_object_mut() {
                fields.insert(
                    "snapshot_iso8601".to_string(),
                    serde_json::json!(snapshot_utc.to_rfc3339()),
                );
            };

            let followers_count = value
                .get("followers_count")
                .and_then(|followers_count_value| followers_count_value.as_u64())
                .expect(&format!("Invalid followers_count: {}", value).to_string());

            let entry = by_id.entry(id);

            entry
                .and_modify(|pair| {
                    if snapshot < pair.0 {
                        *pair = (snapshot, followers_count, value.clone());
                    }
                })
                .or_insert((snapshot, followers_count, value));
        }
    }

    let mut values = by_id
        .into_iter()
        .map(|(key, (_, followers_count, value))| (key, followers_count, value))
        .collect::<Vec<_>>();
    values.sort_by_key(|(key, followers_count, _)| (Reverse(*followers_count), *key));

    for (_, _, value) in values {
        println!("{}", value);
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "select", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Data file path
    #[clap(long)]
    data: String,
    /// Exclusion file path
    #[clap(long)]
    exclusions: Option<String>,
}
