use egg_mode_extras::client::{Client, TokenType};
use hst_cli::prelude::*;
use hst_tw_follow::session::{RunInfo, Session};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let session = Session::open(
        Client::from_config_file(opts.keys)
            .await
            .map_err(hst_tw_follow::session::Error::from)?,
        opts.store,
        opts.tracked,
        opts.deactivations,
    )?;

    match opts.command {
        Command::Run => loop {
            match session.run(TokenType::App).await? {
                Some(RunInfo::Archived {
                    archived_batch_count,
                }) => {
                    log::info!("Archived {} batches", archived_batch_count);
                }
                Some(RunInfo::Next(id, diff)) => {
                    log::info!("Next: {}, {}", id, diff.num_days());
                }
                Some(other) => {
                    log::info!("Unknown result: {:?}", other);
                }
                None => {
                    break;
                }
            }
        },
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Session error")]
    Session(#[from] hst_tw_follow::session::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

#[derive(Debug, Parser)]
#[clap(name = "hst-tw-follow", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// TOML file containing Twitter API keys
    #[clap(long, default_value = "keys.toml")]
    keys: String,
    /// Follow data directory path
    #[clap(long, default_value = "follow-data/")]
    store: String,
    /// Tracked user database file path
    #[clap(long, default_value = "tracked.db")]
    tracked: String,
    /// Deactivation log file path
    #[clap(long, default_value = "deactivations.csv")]
    deactivations: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Run,
}
