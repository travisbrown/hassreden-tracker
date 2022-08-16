use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use flate2::read::GzDecoder;
use futures::{
    future::TryFutureExt,
    stream::{StreamExt, TryStreamExt},
};
use hst_cli::prelude::*;
use hst_tw_profiles::model::User;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
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
        Command::ValidatePair { raw, gz } => {
            validate_pair(&raw, &gz)?;
        }
        Command::ValidatePairs { base } => {
            let mut zst_files = std::fs::read_dir(base)?
                .map(|result| result.map(|entry| entry.path()))
                .collect::<Result<Vec<_>, _>>()?;
            zst_files.sort();

            futures::stream::iter(zst_files)
                .map(|path| {
                    Ok(tokio::spawn(async move { validate_zst(&path) })
                        .map_ok_or_else(|error| Err(Error::from(error)), |result| result))
                })
                .try_buffer_unordered(8)
                .try_for_each(|(date, count)| async move {
                    println!("{},{}", date, count);

                    Ok(())
                })
                .await?;

            /*for zst_path in zst_files {
                let count = validate_zst(&zst_path)?;
                println!("{:?}: {}", zst_path, count);
            }*/
        }
    }

    Ok(())
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
    /// Validate a pair
    ValidatePair {
        /// Uncompressed input path
        #[clap(long)]
        raw: String,
        /// Compressed input path
        #[clap(long)]
        gz: String,
    },
    /// Validate a directory pair
    ValidatePairs {
        /// Directory path
        #[clap(long)]
        base: String,
    },
}

fn validate_zst<P: AsRef<Path>>(path: P) -> Result<(NaiveDate, usize), Error> {
    let date = extract_path_date(&path)?;
    let reader = BufReader::new(zstd::stream::read::Decoder::new(File::open(path)?)?);
    let mut count = 0;
    let mut last_snapshot = 0;
    let mut last_user_id = 0;

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
    }

    Ok((date, count))
}

fn validate_pair<P: AsRef<Path>>(raw: P, gz: P) -> Result<(), Error> {
    let raw_date = extract_path_date(&raw)?;
    let gz_date = extract_path_date(&gz)?;

    if raw_date != gz_date {
        panic!("Dates don't match");
    }
    println!("Processing: {}", raw_date);

    let raw_reader = BufReader::new(File::open(&raw)?);
    let gz_reader = BufReader::new(GzDecoder::new(File::open(&gz)?));
    let mut gz_lines = gz_reader.lines();
    let mut count = 0;
    let mut last_snapshot = 0;
    let mut last_user_id = 0;

    for (i, raw_line) in raw_reader.lines().enumerate() {
        let raw_line = raw_line?;
        if let Some(gz_line) = gz_lines.next() {
            let gz_line = gz_line?;

            if raw_line == gz_line {
                let profile = serde_json::from_str::<User>(&raw_line)?;

                if profile.snapshot < last_snapshot {
                    panic!("Invalid snapshot at line: {}", i + 1);
                } else if profile.snapshot == last_snapshot && profile.id() <= last_user_id {
                    panic!("Invalid user ID at line: {}", i + 1);
                }

                if profile.snapshot().date_naive() != raw_date {
                    panic!("Invalid snapshot at line: {}", i + 1);
                } else {
                    count += 1;
                }

                last_snapshot = profile.snapshot;
                last_user_id = profile.id();
            } else {
                panic!("Unmatched lines at line: {}", i + 1);
            }
        } else {
            panic!("Missing data from gz at line: {}", i + 1);
        }
    }

    if gz_lines.next().is_some() {
        panic!("Extra data from gz");
    }

    println!("Success: {} snapshots for {}", count, raw_date);

    Ok(())
}
