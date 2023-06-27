use chrono::{TimeZone, Utc};
use hst_cli::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Record {
    id: u64,
    snapshot: i64,
}

impl Record {
    fn from_json(value: &Value) -> Option<Self> {
        let id = value.get("id_str")?.as_str()?.parse::<u64>().ok()?;
        let snapshot = value.get("snapshot")?.as_i64()?;

        Some(Record { id, snapshot })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let include_ids = match opts.include {
        Some(file_name) => {
            let mut ids = std::collections::HashSet::new();
            let file = std::fs::File::open(file_name)?;
            for line in std::io::BufReader::new(file).lines() {
                let line = line?;
                let id = line.parse::<u64>().unwrap();
                ids.insert(id);
            }

            Some(ids)
        }
        None => None,
    };

    let mut seen = HashMap::<u64, (i64, String)>::new();

    let reader = BufReader::new(File::open(opts.input)?);

    for line in reader.lines() {
        let line = line?;
        if line.starts_with('{') {
            let json = serde_json::from_str(&line)?;
            let record =
                Record::from_json(&json).unwrap_or_else(|| panic!("Invalid line {}", line));

            // Special case: make sure the result isn't broken
            if line.contains("ext_is_blue_verified")
                && seen
                    .get(&record.id)
                    .filter(|(prev_snapshot, _)| *prev_snapshot >= record.snapshot)
                    .is_none()
            {
                seen.insert(record.id, (record.snapshot, line));
            }
        } else {
            let parts = line.split(',').collect::<Vec<_>>();
            let id = parts[0].parse::<u64>().unwrap();
            let snapshot = parts[2].parse::<i64>().unwrap();

            if seen
                .get(&id)
                .filter(|(prev_snapshot, _)| *prev_snapshot >= snapshot)
                .is_none()
            {
                seen.insert(id, (snapshot, line));
            }
        }
    }

    let mut by_id = seen.into_iter().collect::<Vec<_>>();
    by_id.sort_by_key(|(id, _)| *id);

    for (id, (_, line)) in by_id {
        let include = include_ids
            .as_ref()
            .map(|ids| ids.contains(&id))
            .unwrap_or(true);
        if include {
            println!("{}", line);
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "dedup", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Data file path
    #[clap(long)]
    input: String,
    /// Allowlist file path
    #[clap(long)]
    include: Option<String>,
}
