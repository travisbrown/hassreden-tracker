use clap::Parser;
use std::fs::File;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = hst_cli::init_logging(opts.verbose)?;
    let log = hst_deactivations::DeactivationLog::read(File::open(opts.deactivations)?)?;

    if let Err(invalid_user_ids) = log.validate() {
        for user_id in invalid_user_ids {
            log::error!("Invalid user ID: {}", user_id);
        }
    }

    let mut suspended_user_ids = log
        .current_deactivated(Some(63))
        .into_iter()
        .collect::<Vec<_>>();
    suspended_user_ids.sort_unstable();

    match opts.command {
        Command::Run => {
            for user_id in suspended_user_ids {
                // We expect there to be a non-empty list of entries.
                if let Some(entries) = log.lookup(user_id) {
                    let self_deactivations_observed =
                        entries.iter().filter(|entry| entry.status == 50).count();
                    let suspensions_observed =
                        entries.iter().filter(|entry| entry.status == 63).count();

                    if let Some(last_entry) = entries.last() {
                        println!(
                            "{},{},{},{}",
                            user_id,
                            last_entry.observed.timestamp(),
                            self_deactivations_observed,
                            suspensions_observed
                        )
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("ProfileDb error")]
    ProfileDb(#[from] hst_tw_db::Error),
    /*#[error("Profile Avro error")]
    ProfileAvro(#[from] twprs::avro::Error),
    #[error("Avro decoding error")]
    Avro(#[from] apache_avro::Error),
    #[error("JSON encoding error")]
    Json(#[from] serde_json::Error),*/
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
    #[error("Deactivation file parsing error")]
    DeactivationFile(#[from] hst_deactivations::Error),
}

#[derive(Debug, Parser)]
#[clap(name = "suspensions", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Database directory path
    #[clap(long)]
    db: String,
    /// Deactivations file path
    #[clap(long)]
    deactivations: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Run,
}
