use chrono::{Duration, Utc};
use egg_mode_extras::client::{Client, TokenType};
use futures::try_join;
use hst_cli::prelude::*;
use hst_tw_db::{table::ReadOnly, ProfileDb};
use hst_tw_follow::{
    downloader::Downloader,
    session::{RunInfo, Session},
};

const ERROR_WAIT_S: u64 = 10 * 60;
const MIN_AGE_S: i64 = 6 * 60 * 60;

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
                download_loop(&downloader, batch, TokenType::App),
                download_loop(&downloader, batch, TokenType::User)
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
            let count = session.clean_profile_ages()?;

            log::info!("Removed {} users from the queue", count);
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
    #[clap(long, default_value = "data/follows/")]
    store: String,
    /// Tracked user database file path
    #[clap(long, default_value = "data/tracked.db")]
    tracked: String,
    /// Deactivation log file path
    #[clap(long, default_value = "data/deactivations.csv")]
    deactivations: String,
    /// Profile age database path
    #[clap(long, default_value = "data/profile-ages-db/")]
    ages: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Run {
        /// Downloader base directory path
        #[clap(long, default_value = "data/profiles")]
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
        match session.run(token_type).await {
            Ok(info) => match info {
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
            },
            Err(error) => {
                log::error!("Follows error: {:?}", error);
                if !wait_for_window_follow(&error).await {
                    tokio::time::sleep(tokio::time::Duration::from_secs(ERROR_WAIT_S)).await;
                }
            }
        }
    }
}

async fn download_loop(
    downloader: &Downloader,
    count: usize,
    token_type: TokenType,
) -> Result<(), Error> {
    loop {
        log::info!("Beginning download: {}", count);
        match downloader.run_batch(count, token_type).await.and_then(
            |(deactivated_count, profile_count)| {
                downloader
                    .profile_age_db
                    .queue_status(Duration::seconds(MIN_AGE_S))
                    .map_err(|error| error.into())
                    .map(|(prioritized_count, first_next)| {
                        (
                            deactivated_count,
                            profile_count,
                            prioritized_count,
                            first_next,
                        )
                    })
            },
        ) {
            Ok((deactivated_count, profile_count, prioritized_count, first_next)) => {
                log::info!(
                    "Download: {} profiles, {} deactivated; queue: {} prioritized, scheduled: {}",
                    profile_count,
                    deactivated_count,
                    prioritized_count,
                    first_next
                );

                if deactivated_count == 0 && profile_count == 0 {
                    tokio::time::sleep(tokio::time::Duration::from_secs(ERROR_WAIT_S)).await;
                }
            }
            Err(error) => {
                log::error!("Downloader error: {:?}", error);
                if !wait_for_window_downloader(&error).await {
                    tokio::time::sleep(tokio::time::Duration::from_secs(ERROR_WAIT_S)).await;
                }
            }
        }
    }
}

const WAIT_BUFFER_S: u64 = 30;

async fn wait_for_window_follow(error: &hst_tw_follow::session::Error) -> bool {
    match error {
        hst_tw_follow::session::Error::EggMode(egg_mode::error::Error::RateLimit(timestamp_s)) => {
            let now_s = Utc::now().timestamp() as u64;
            let diff_s = *timestamp_s as u64 - now_s + WAIT_BUFFER_S;

            if diff_s > 0 {
                tokio::time::sleep(tokio::time::Duration::from_secs(diff_s)).await;
            }

            true
        }
        _ => false,
    }
}

async fn wait_for_window_downloader(error: &hst_tw_follow::downloader::Error) -> bool {
    match error {
        hst_tw_follow::downloader::Error::EggMode(egg_mode::error::Error::RateLimit(
            timestamp_s,
        )) => {
            let now_s = Utc::now().timestamp() as u64;
            let diff_s = *timestamp_s as u64 - now_s + WAIT_BUFFER_S;

            if diff_s > 0 {
                tokio::time::sleep(tokio::time::Duration::from_secs(diff_s)).await;
            }

            true
        }
        _ => false,
    }
}
