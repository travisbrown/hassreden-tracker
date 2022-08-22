use hst_cli::prelude::*;
use hst_tw_db::{
    table::{ReadOnly, Table},
    ProfileDb,
};
use hst_tw_follow::dbs::tracked::TrackedUserDb;
use std::collections::HashSet;
use std::io::BufRead;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let db = TrackedUserDb::open(opts.db)?;

    match opts.command {
        Command::Update { profiles } => {
            let profile_db = ProfileDb::<ReadOnly>::open(profiles, false)?;
            let missing_ids = db.update_all(&profile_db, None)?;

            for missing_id in missing_ids {
                log::warn!("No profile for {}", missing_id);
            }
        }
        Command::Create { profiles } => {
            let ids = std::io::stdin()
                .lock()
                .lines()
                .map(|result| {
                    result.map_err(Error::from).and_then(|line| {
                        line.parse::<u64>()
                            .map_err(|_| Error::InvalidId(line.clone()))
                    })
                })
                .collect::<Result<HashSet<_>, _>>()?;

            let profile_db = ProfileDb::<ReadOnly>::open(profiles, false)?;
            let missing_ids = db.update_all(&profile_db, Some(ids))?;

            for missing_id in missing_ids {
                log::warn!("No profile for {}", missing_id);
            }
        }
        Command::Export => {
            for (id, screen_name, target_age) in db.export()? {
                println!(
                    "{},{},{}",
                    id,
                    screen_name,
                    target_age
                        .map(|duration| duration.num_seconds().to_string())
                        .unwrap_or_default()
                );
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Invalid ID")]
    InvalidId(String),
    #[error("ProfileDb error")]
    ProfileDb(#[from] hst_tw_db::Error),
    #[error("TrackedUserDb error")]
    TrackedUserDb(#[from] hst_tw_follow::dbs::tracked::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

#[derive(Debug, Parser)]
#[clap(name = "hst-tw-follow-tracked-db", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Database path
    #[clap(long)]
    db: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Update {
        /// Profile database path
        #[clap(short, long)]
        profiles: String,
    },
    Create {
        /// Profile database path
        #[clap(short, long)]
        profiles: String,
    },
    Export,
}
