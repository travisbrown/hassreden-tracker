use hst_cli::prelude::*;
use hst_tw_follow::formats::{archive::write_batches, legacy, transform::deduplicate_removals};
use hst_tw_follow::{store::Store, Batch};
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
