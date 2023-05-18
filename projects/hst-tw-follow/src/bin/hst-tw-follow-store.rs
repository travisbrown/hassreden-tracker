use chrono::{DateTime, TimeZone, Utc};
use hst_cli::prelude::*;
use hst_deactivations::DeactivationLog;
use hst_tw_follow::formats::{archive::write_batches, legacy, transform::deduplicate_removals};
use hst_tw_follow::{store::Store, Batch};
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufWriter;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let store = Store::open(opts.store)?;

    match opts.command {
        Command::Archive => {
            store.archive()?;
        }
        Command::Validate => {
            store.validate()?;
        }
        Command::BatchInfo => {
            for result in store.past_batches() {
                let (_, batch) = result?;
                print_batch(&batch);
            }

            for batch in store.current_batches()? {
                print_batch(&batch);
            }
        }
        Command::ByUser => {
            let mut by_user = HashMap::<u64, Vec<DateTime<Utc>>>::new();

            for result in store.past_batches() {
                let (_, batch) = result?;
                let dates = by_user.entry(batch.user_id).or_default();
                dates.push(batch.timestamp);
            }

            let mut by_user_vec = by_user.into_iter().collect::<Vec<_>>();
            by_user_vec.sort_by_key(|(id, _)| *id);

            for (id, dates) in by_user_vec {
                let dates = dates
                    .into_iter()
                    .map(|date| date.timestamp().to_string())
                    .collect::<Vec<_>>();
                println!("{},{}", id, dates.join(","));
            }
        }
        Command::TimeScores { window, count } => {
            let date_counts = store.new_addition_report(window as usize, Some(0.25), None)?;

            for (date, by_user) in date_counts {
                let mut by_user = by_user.into_iter().collect::<Vec<_>>();
                by_user.sort_by_key(|(id, ((new_follower_count, _), (_, _)))| {
                    Reverse((*new_follower_count, *id))
                });

                for (
                    id,
                    (
                        (new_follower_count, total_follower_count),
                        (new_followed_count, total_followed_count),
                    ),
                ) in by_user.iter().take(count)
                {
                    println!(
                        "{},{},{},{},{},{}",
                        date,
                        id,
                        new_follower_count,
                        total_follower_count,
                        new_followed_count,
                        total_followed_count
                    );
                }
            }
        }
        Command::Dump => {
            for (id, follower_ids) in store.followers() {
                println!(
                    "{},{}",
                    id,
                    follower_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }

            for (id, following_ids) in store.following() {
                println!(
                    "{},{}",
                    id,
                    following_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }
        }
        Command::ExportFollowers { id } => {
            for id in store.user_followers(id).unwrap_or_default() {
                println!("{}", id);
            }
        }
        Command::ExportFollowing { id } => {
            for id in store.user_following(id).unwrap_or_default() {
                println!("{}", id);
            }
        }
        Command::IdsSince {
            deactivations,
            timestamp,
        } => {
            let mut ids = HashSet::new();
            let beginning = Utc
                .timestamp_opt(timestamp, 0)
                .single()
                .ok_or(Error::InvalidTimestamp(timestamp))?;
            let log = match deactivations {
                Some(path) => DeactivationLog::read(File::open(path)?)?,
                None => DeactivationLog::default(),
            };

            for result in store.past_batches() {
                let (_, batch) = result?;
                if batch.timestamp >= beginning {
                    for id in batch.addition_ids() {
                        ids.insert(id);
                    }

                    for id in batch.removal_ids() {
                        if log.status(id).is_none() {
                            ids.insert(id);
                        }
                    }
                }
            }

            for batch in store.current_batches()? {
                if batch.timestamp >= beginning {
                    for id in batch.addition_ids() {
                        ids.insert(id);
                    }

                    for id in batch.removal_ids() {
                        if log.status(id).is_none() {
                            ids.insert(id);
                        }
                    }
                }
            }

            let mut ids = ids.into_iter().collect::<Vec<_>>();
            ids.sort_unstable();

            for id in ids {
                println!("{}", id);
            }
        }
        Command::Scores => {
            let mut scores = store.user_scores()?.into_iter().collect::<Vec<_>>();
            scores.sort_by_key(|(id, score)| (Reverse(*score), *id));

            for (id, score) in scores {
                println!("{},{}", id, score);
            }
        }
        Command::ConvertLegacy { input, output } => {
            let batches = deduplicate_removals(legacy::read_batches(input));
            let mut writer = BufWriter::new(File::create(output)?);
            write_batches(&mut writer, batches)?;
        }
        Command::KnownUsers => {
            let mut known_user_ids = store.known_user_ids()?.into_iter().collect::<Vec<_>>();
            known_user_ids.sort();

            for id in known_user_ids {
                println!("{}", id);
            }
        }
        Command::Lookup { id } => {
            for result in store.past_batches() {
                let (date, batch) = result?;

                if let Some(change) = &batch.follower_change {
                    if change.addition_ids.contains(&id) {
                        println!(
                            ">,+,{},{},{}",
                            batch.user_id,
                            batch.timestamp.timestamp(),
                            date
                        );
                    }
                    if change.removal_ids.contains(&id) {
                        println!(
                            ">,-,{},{},{}",
                            batch.user_id,
                            batch.timestamp.timestamp(),
                            date
                        );
                    }
                }

                if let Some(change) = &batch.followed_change {
                    if change.addition_ids.contains(&id) {
                        println!(
                            "<,+,{},{},{}",
                            batch.user_id,
                            batch.timestamp.timestamp(),
                            date
                        );
                    }
                    if change.removal_ids.contains(&id) {
                        println!(
                            "<,-,{},{},{}",
                            batch.user_id,
                            batch.timestamp.timestamp(),
                            date
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Store error")]
    Store(#[from] hst_tw_follow::store::Error),
    #[error("Legacy format error")]
    Legacy(#[from] hst_tw_follow::formats::legacy::Error),
    #[error("Deactivation file error")]
    Deactivations(#[from] hst_deactivations::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
    #[error("Invalid timestamp")]
    InvalidTimestamp(i64),
}

#[derive(Debug, Parser)]
#[clap(name = "hst-tw-follow-store", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Follow data directory path
    #[clap(long, default_value = "data/follows")]
    store: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Archive,
    Validate,
    BatchInfo,
    Dump,
    ExportFollowers {
        id: u64,
    },
    ExportFollowing {
        id: u64,
    },
    IdsSince {
        /// Deactivations file path
        #[clap(long)]
        deactivations: Option<String>,
        /// Epoch second
        timestamp: i64,
    },
    Scores,
    ByUser,
    TimeScores {
        #[clap(long, default_value = "1")]
        window: u8,
        #[clap(long, default_value = "10")]
        count: usize,
    },
    ConvertLegacy {
        /// Input data directory path
        #[clap(long)]
        input: String,
        /// Output file path
        #[clap(long, default_value = "current.bin")]
        output: String,
    },
    KnownUsers,
    Lookup {
        id: u64,
    },
}

fn print_batch(batch: &Batch) {
    let (follower_ac, follower_rc) = batch
        .follower_change
        .as_ref()
        .map(|change| {
            (
                change.addition_ids.len().to_string(),
                change.removal_ids.len().to_string(),
            )
        })
        .unwrap_or_default();

    let (followed_ac, followed_rc) = batch
        .followed_change
        .as_ref()
        .map(|change| {
            (
                change.addition_ids.len().to_string(),
                change.removal_ids.len().to_string(),
            )
        })
        .unwrap_or_default();

    println!(
        "{},{},{},{},{},{}",
        batch.timestamp, batch.user_id, follower_ac, follower_rc, followed_ac, followed_rc
    );
}
