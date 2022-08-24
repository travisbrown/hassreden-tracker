use egg_mode_extras::client::{Client, TokenType};
use futures::try_join;
use hst_cli::prelude::*;
use hst_tw_db::{table::ReadOnly, ProfileDb};
use hst_tw_follow::{
    downloader::Downloader,
    session::{RunInfo, Session},
};

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
        opts.ages,
    )?;

    match opts.command {
        Command::Run { download, batch } => {
            let downloader = session.downloader(&download);

            try_join!(
                run_loop(&session, TokenType::App),
                run_loop(&session, TokenType::User),
                download_loop(&downloader, batch)
            )?;
        }
        Command::Scrape { user_token, id } => {
            let token_type = if user_token {
                TokenType::User
            } else {
                TokenType::App
            };
            let info = session.scrape(token_type, id, None).await?;
            println!("{:?}", info);
        }
        Command::ValidateTrackedDb => {
            let (store_only_ids, tracked_db_only_ids) = session.compare_users()?;

            println!(
                "Only in store: {}",
                store_only_ids
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            println!(
                "Only in tracked user database: {}",
                tracked_db_only_ids
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        Command::ReloadAges { profiles } => {
            let profile_db = ProfileDb::<ReadOnly>::open(profiles, false)?;
            session.reload_profile_ages(&profile_db)?;
        }
        Command::CleanAges => {
            session.clean_profile_ages()?;
        }
        Command::DumpDownloaderQueue { count } => {
            let items = session
                .profile_age_db
                .dump_next(count)
                .map_err(hst_tw_follow::session::Error::from)?;

            for (id, next, last, started) in items {
                println!(
                    "{},{},{},{}",
                    id,
                    next.map(|value| value.to_string()).unwrap_or_default(),
                    last.map(|value| value.to_string()).unwrap_or_default(),
                    started.map(|value| value.to_string()).unwrap_or_default(),
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
    #[error("Session error")]
    Session(#[from] hst_tw_follow::session::Error),
    #[error("ProfileDb error")]
    ProfileDb(#[from] hst_tw_db::Error),
    #[error("Downloader error")]
    Downloader(#[from] hst_tw_follow::downloader::Error),
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
    /// Profile age database path
    #[clap(long, default_value = "profile-ages-db/")]
    ages: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Run {
        /// Downloader base directory path
        #[clap(long, default_value = "profiles")]
        download: String,
        /// Downloader batch size
        #[clap(long, default_value = "20000")]
        batch: usize,
    },
    Scrape {
        /// Use user token
        #[clap(long)]
        user_token: bool,
        /// User ID
        id: u64,
    },
    ValidateTrackedDb,
    ReloadAges {
        /// Profile database path
        #[clap(short, long)]
        profiles: String,
    },
    CleanAges,
    DumpDownloaderQueue {
        #[clap(long, default_value = "100")]
        count: usize,
    },
}

async fn run_loop(session: &Session, token_type: TokenType) -> Result<(), Error> {
    let tag = if token_type == TokenType::App {
        "[APPL]"
    } else {
        "[USER]"
    };
    loop {
        match session.run(token_type).await? {
            Some(RunInfo::Archived {
                archived_batch_count,
            }) => {
                log::info!("{} Archived {} batches", tag, archived_batch_count);
            }
            Some(RunInfo::Scraped { batch }) => {
                log::info!("{} Batch: {}, {}", tag, batch.user_id, batch.total_len());
            }
            Some(other) => {
                log::info!("{} Other result: {:?}", tag, other);
            }
            None => {
                log::info!("{} Empty result", tag);
            }
        }
    }
}

async fn download_loop(downloader: &Downloader, count: usize) -> Result<(), Error> {
    loop {
        downloader.run_batch(count).await?;
    }
}
