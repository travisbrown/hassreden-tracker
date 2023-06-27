use chrono::{Date, DateTime, NaiveDate, TimeZone, Utc};
use hst_cli::prelude::*;
use hst_deactivations::DeactivationLog;
use hst_tw_utils::parse_date_time;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

fn from_json(value: &Value) -> Option<(u64, DateTime<Utc>, DateTime<Utc>, String, usize)> {
    let id = value.get("id_str")?.as_str()?.parse::<u64>().ok()?;
    let status = value.get("status")?.as_object()?;
    let created_at = status.get("created_at")?.as_str()?;
    let parsed = parse_date_time(created_at).ok()?;
    let snapshot_s = value.get("snapshot")?.as_i64()?;
    let snapshot = Utc.timestamp_opt(snapshot_s, 0).single()?;
    let followers_count = value.get("followers_count")?.as_u64()? as usize;
    let screen_name = value.get("screen_name")?.as_str()?.to_string();

    Some((id, snapshot, parsed, screen_name, followers_count))
}

fn list_files<P: AsRef<Path>>(
    path: P,
) -> Result<Vec<(String, PathBuf)>, Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        results.push((file_name, path));
    }

    Ok(results)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let mut deactivations = DeactivationLog::read(File::open("deactivations.csv")?)?;
    let mut deactivations_by_date = deactivations
        .deactivations(None)
        .into_iter()
        .map(|(id, entry)| (entry.observed, id))
        .collect::<Vec<_>>();
    deactivations_by_date.sort();
    deactivations_by_date.reverse();

    let paths = opts.data.split(',').collect::<Vec<_>>();
    let mut by_file_name = HashMap::new();

    for dir_path in paths {
        for (file_name, path) in list_files(dir_path)? {
            by_file_name.insert(file_name, path);
        }
    }

    let mut by_file_name = by_file_name.into_iter().collect::<Vec<_>>();
    by_file_name.sort_by_key(|(file_name, _)| file_name.clone());

    let mut last_tweeted = HashMap::new();
    let mut last_info = HashMap::new();

    for (file_name, path) in by_file_name {
        if opts
            .start
            .as_ref()
            .map(|start_file_name| start_file_name <= &file_name)
            .unwrap_or(true)
        {
            log::info!("Reading {:?}", path);
            let reader = BufReader::new(zstd::stream::read::Decoder::new(File::open(path)?)?);

            for (i, line) in reader.lines().enumerate() {
                let line = line?;
                let json = serde_json::from_str(&line)?;
                if let Some((id, snapshot, last, screen_name, follower_count)) = from_json(&json) {
                    if last_tweeted
                        .get(&id)
                        .map(|prev| prev < &last)
                        .unwrap_or(true)
                    {
                        last_tweeted.insert(id, last);
                    }

                    if last_info
                        .get(&id)
                        .map(|(prev, _)| prev < &snapshot)
                        .unwrap_or(true)
                    {
                        last_info.insert(id, (snapshot, (screen_name, follower_count)));
                    }
                } else {
                    log::error!("Invalid record: {}", json);
                }
            }
        }
    }

    let mut sorted = last_tweeted.into_iter().collect::<Vec<_>>();
    sorted.sort();

    for (id, last) in sorted {
        let (_, (screen_name, follower_count)) = last_info.get(&id).unwrap();
        println!(
            "{},{},{},{},{}",
            id,
            screen_name,
            follower_count,
            last.timestamp(),
            last.date_naive()
        );
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "inactive", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Data directory paths
    #[clap(long)]
    data: String,
    /// First date
    #[clap(long)]
    start: Option<String>,
}
