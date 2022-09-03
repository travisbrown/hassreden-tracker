use chrono::NaiveDate;
use futures::{
    future::TryFutureExt,
    stream::{StreamExt, TryStreamExt},
};
use hst_cli::prelude::*;
use hst_tw_profiles::model::User;
use regex::Regex;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("Invalid file path")]
    InvalidPath(Box<Path>),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
    #[error("Task error")]
    Task(#[from] tokio::task::JoinError),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::Validate { base, filter } => {
            let mut zst_files = std::fs::read_dir(base)?
                .map(|result| result.map(|entry| entry.path()))
                .collect::<Result<Vec<_>, _>>()?;

            if let Some(filter_re) = filter {
                zst_files.retain(|path| filter_re.is_match(&path.to_string_lossy()));
            }
            zst_files.sort();

            futures::stream::iter(zst_files)
                .map(|path| {
                    Ok(tokio::spawn(async move { validate_zst(&path) })
                        .map_ok_or_else(|error| Err(Error::from(error)), |result| result))
                })
                .try_buffer_unordered(8)
                .try_for_each(|(date, profile_count, user_count)| async move {
                    println!("{},{},{}", date, profile_count, user_count);

                    Ok(())
                })
                .await?;
        }
    }

    Ok(())
}

#[derive(Parser)]
#[clap(name = "hst-tw-profiles", about, version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    /// Validate a directory of ZSTD-compressed ND-JSON files
    Validate {
        /// Directory path
        #[clap(long)]
        base: String,
        /// File name filter
        #[clap(long)]
        filter: Option<Regex>,
    },
}

fn extract_path_date<P: AsRef<Path>>(path: P) -> Result<NaiveDate, Error> {
    let date_str = path
        .as_ref()
        .file_name()
        .and_then(|ostr| ostr.to_str())
        .and_then(|str| str.split('.').next())
        .ok_or_else(|| Error::InvalidPath(path.as_ref().to_path_buf().into_boxed_path()))?;

    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|_| Error::InvalidPath(path.as_ref().to_path_buf().into_boxed_path()))
}

fn validate_zst<P: AsRef<Path>>(path: P) -> Result<(NaiveDate, usize, usize), Error> {
    let date = extract_path_date(&path)?;
    let reader = BufReader::new(zstd::stream::read::Decoder::new(File::open(path)?)?);
    let mut count = 0;
    let mut last_snapshot = 0;
    let mut last_user_id = 0;
    let mut user_ids = HashSet::new();

    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        let profile = serde_json::from_str::<User>(&line)?;

        if profile.snapshot < last_snapshot {
            panic!("Invalid snapshot at line: {}", i + 1);
        } else if profile.snapshot == last_snapshot && profile.id() <= last_user_id {
            panic!("Invalid user ID at line: {}", i + 1);
        }

        if profile.snapshot().date_naive() != date {
            panic!("Invalid snapshot at line: {}", i + 1);
        } else {
            count += 1;
        }

        last_snapshot = profile.snapshot;
        last_user_id = profile.id();
        user_ids.insert(last_user_id);
    }

    Ok((date, count, user_ids.len()))
}
