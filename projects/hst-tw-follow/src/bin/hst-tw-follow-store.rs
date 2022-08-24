use chrono::{TimeZone, Utc};
use hst_cli::prelude::*;
use hst_deactivations::DeactivationLog;
use hst_tw_follow::formats::{archive::write_batches, legacy, transform::deduplicate_removals};
use hst_tw_follow::{store::Store, Batch};
use std::collections::HashSet;
use std::fs::File;
use std::io::BufWriter;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let store = Store::open(opts.store)?;

    match opts.command {
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
        Command::IdsSince {
            deactivations,
            timestamp,
        } => {
            let mut ids = HashSet::new();
            let beginning = Utc.timestamp(timestamp, 0);
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
        Command::ConvertLegacy { input, output } => {
            let batches = deduplicate_removals(legacy::read_batches(input));
            let mut writer = BufWriter::new(File::create(output)?);
            write_batches(&mut writer, batches)?;
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
}

#[derive(Debug, Parser)]
#[clap(name = "hst-tw-follow-store", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Follow data directory path
    #[clap(long, default_value = "follow-data/")]
    store: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Validate,
    BatchInfo,
    Dump,
    IdsSince {
        /// Deactivations file path
        #[clap(long)]
        deactivations: Option<String>,
        /// Epoch second
        timestamp: i64,
    },
    ConvertLegacy {
        /// Input data directory path
        #[clap(long)]
        input: String,
        /// Output file path
        #[clap(long, default_value = "current.bin")]
        output: String,
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
