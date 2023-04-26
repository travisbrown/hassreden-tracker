use chrono::{Date, DateTime, NaiveDate, TimeZone, Utc};
use hst_cli::prelude::*;
use hst_deactivations::DeactivationLog;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use zstd::stream::read::Decoder as ZstDecoder;

#[derive(Clone, Debug, Eq, PartialEq)]
enum VerifiedType {
    Business,
    Government,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Record {
    id: u64,
    snapshot: DateTime<Utc>,
    followers_count: usize,
    verified: bool,
    blue_verified: Option<bool>,
    verified_type: Option<VerifiedType>,
}

impl Record {
    fn from_json(value: &Value) -> Option<Self> {
        let id = value.get("id_str")?.as_str()?.parse::<u64>().ok()?;
        let snapshot_s = value.get("snapshot")?.as_i64()?;
        let followers_count = value.get("followers_count")?.as_u64()? as usize;
        let verified = value.get("verified")?.as_bool()?;
        let blue_verified = value
            .get("ext_is_blue_verified")
            .and_then(|value| value.as_bool());
        let verified_type = value
            .get("ext_verified_type")
            .and_then(|verified_type_value| {
                verified_type_value.as_str().and_then(|str| match str {
                    "Government" => Some(VerifiedType::Government),
                    "Business" => Some(VerifiedType::Business),
                    _ => None,
                })
            });

        let snapshot = Utc.timestamp_opt(snapshot_s, 0).single()?;

        Some(Record {
            id,
            snapshot,
            followers_count,
            verified,
            blue_verified,
            verified_type,
        })
    }

    fn date(&self) -> NaiveDate {
        self.snapshot.date_naive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Counts {
    verified: usize,
    blue_verified: usize,
    government: usize,
    business: usize,
}

impl Counts {
    fn new(verified: usize, blue_verified: usize, government: usize, business: usize) -> Self {
        Self {
            verified,
            blue_verified,
            government,
            business,
        }
    }
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

fn write_date(
    date: NaiveDate,
    followers_counts: &HashMap<u64, usize>,
    blue_verified_ids: &HashSet<u64>,
    verified_ids: &HashSet<u64>,
    government_ids: &HashSet<u64>,
    business_ids: &HashSet<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_name = format!("dates/{}.txt", date);

    let mut writer = BufWriter::new(File::create(file_name)?);

    writeln!(
        writer,
        "{},{},{},{}",
        blue_verified_ids.len(),
        verified_ids.len(),
        government_ids.len(),
        business_ids.len()
    )?;

    let mut ids = blue_verified_ids.iter().collect::<Vec<_>>();
    ids.sort();

    for id in ids {
        writeln!(writer, "{},{}", id, followers_counts.get(id).unwrap())?;
    }

    let mut ids = verified_ids.iter().collect::<Vec<_>>();
    ids.sort();

    for id in ids {
        writeln!(writer, "{},{}", id, followers_counts.get(id).unwrap())?;
    }

    let mut ids = government_ids.iter().collect::<Vec<_>>();
    ids.sort();

    for id in ids {
        writeln!(writer, "{},{}", id, followers_counts.get(id).unwrap())?;
    }

    let mut ids = business_ids.iter().collect::<Vec<_>>();
    ids.sort();

    for id in ids {
        writeln!(writer, "{},{}", id, followers_counts.get(id).unwrap())?;
    }

    Ok(())
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

    let mut by_date: HashMap<NaiveDate, Counts> = HashMap::new();
    let mut verified_ids = HashSet::new();
    let mut blue_verified_ids = HashSet::new();
    let mut government_ids = HashSet::new();
    let mut business_ids = HashSet::new();
    let mut followers_counts = HashMap::new();

    let mut last_snapshot = DateTime::<Utc>::MIN_UTC;

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
                if let Some(mut record) = Record::from_json(&json) {
                    if record.snapshot < last_snapshot {
                        log::error!("Invalid snapshot ordering at line {}", i);
                    }

                    let last_date = last_snapshot.date_naive();

                    if record.snapshot.date_naive() != last_date
                        && last_snapshot > DateTime::<Utc>::MIN_UTC
                    {
                        let counts = Counts::new(
                            verified_ids.len(),
                            blue_verified_ids.len(),
                            government_ids.len(),
                            business_ids.len(),
                        );

                        by_date.insert(last_date, counts.clone());

                        write_date(
                            last_date,
                            &followers_counts,
                            &blue_verified_ids,
                            &verified_ids,
                            &government_ids,
                            &business_ids,
                        )?;
                    }

                    while deactivations_by_date
                        .last()
                        .map(|(date, _)| date < &record.snapshot)
                        .unwrap_or(false)
                    {
                        if let Some((_, id)) = deactivations_by_date.pop() {
                            blue_verified_ids.remove(&id);
                            verified_ids.remove(&id);
                            government_ids.remove(&id);
                            business_ids.remove(&id);
                        }
                    }

                    if let Some(blue_verified) = record.blue_verified {
                        if blue_verified {
                            blue_verified_ids.insert(record.id);
                            followers_counts.insert(record.id, record.followers_count);

                            if record.verified {
                                verified_ids.insert(record.id);
                            } else {
                                verified_ids.remove(&record.id);
                            }

                            if let Some(verified_type) = record.verified_type {
                                match verified_type {
                                    VerifiedType::Government => {
                                        government_ids.insert(record.id);
                                        business_ids.remove(&record.id);
                                    }
                                    VerifiedType::Business => {
                                        government_ids.remove(&record.id);
                                        business_ids.insert(record.id);
                                    }
                                }
                            } else {
                                government_ids.remove(&record.id);
                                business_ids.remove(&record.id);
                            }
                        } else {
                            blue_verified_ids.remove(&record.id);
                            verified_ids.remove(&record.id);
                            government_ids.remove(&record.id);
                            business_ids.remove(&record.id);
                        }
                    }

                    last_snapshot = record.snapshot;
                } else {
                    log::error!("Invalid record: {}", json);
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "bt", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Data directory paths
    #[clap(long)]
    data: String,
    /// Data directory paths
    #[clap(long)]
    start: Option<String>,
}
