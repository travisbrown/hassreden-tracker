use clap::Parser;
use std::fs::File;
use std::io::BufRead;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    hst_cli::init_logging(opts.verbose)?;
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

    let db = hst_tw_db::ProfileDb::open(opts.db, true)?;

    match opts.command {
        Command::Run => {
            for user_id in suspended_user_ids {
                // We expect there to be a non-empty list of entries.
                if let Some(entries) = log.lookup(user_id) {
                    let self_deactivations_observed =
                        entries.iter().filter(|entry| entry.status == 50).count();
                    let suspensions_observed =
                        entries.iter().filter(|entry| entry.status == 63).count();

                    let profiles = db.lookup(user_id)?;
                    let screen_names = profiles
                        .iter()
                        .map(|(_, _, profile)| profile.screen_name.clone())
                        .collect::<Vec<_>>()
                        .join(";");

                    let last_profile = profiles.last().map(|(_, _, profile)| profile);

                    if let Some(last_entry) = entries.last() {
                        println!(
                            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
                            user_id,
                            screen_names.len(),
                            screen_names,
                            last_entry.observed.timestamp(),
                            last_profile
                                .map(
                                    |profile| (last_entry.observed.timestamp() - profile.snapshot)
                                        .to_string()
                                )
                                .unwrap_or_default(),
                            self_deactivations_observed,
                            suspensions_observed,
                            last_profile
                                .map(|profile| profile.followers_count.to_string())
                                .unwrap_or_default(),
                            last_profile
                                .map(|profile| profile.friends_count.to_string())
                                .unwrap_or_default(),
                            last_profile
                                .map(|profile| profile.statuses_count.to_string())
                                .unwrap_or_default(),
                            last_profile
                                .map(|profile| profile.verified.to_string())
                                .unwrap_or_default(),
                            last_profile
                                .map(|profile| profile.protected.to_string())
                                .unwrap_or_default(),
                            last_profile
                                .map(|profile| profile.withheld_in_countries.join(";"))
                                .unwrap_or_default()
                        )
                    }
                }
            }
        }
        Command::FromIds => {
            let user_ids = std::io::stdin()
                .lock()
                .lines()
                .map(|line| {
                    let line = line?;
                    line.split(',')
                        .next()
                        .and_then(|field| field.parse::<u64>().ok())
                        .ok_or(Error::InvalidId(line))
                })
                .collect::<Result<Vec<_>, _>>()?;

            /*println!(
                r#"<table><tr><th></th><th align="left">Twitter ID</th><th align="left">Screen name</th>"#
            );
            println!(
                r#"<th align="left">Created</th><th align="left">Suspended</th><th align="left">Status</th><th align="left">Follower count</th></tr>"#
            );*/

            println!(
                r#"<table><tr><th></th><th align="left">Twitter ID</th><th align="left">Screen name</th>"#
            );
            println!(
                r#"<th align="left">Created</th><th align="left">Suspended</th><th align="left">Follower count</th></tr>"#
            );

            for user_id in user_ids {
                let result = db.lookup(user_id).ok().and_then(|mut users| {
                    let last = users.pop();

                    last.map(|value| (users, value))
                });

                if let Some(suspension_observed) = log.status_timestamp(user_id) {
                    if let Some((previous, (_, _, user))) = result {
                        let _screen_names = previous
                            .iter()
                            .map(|(_, _, profile)| format!("{}, ", profile.screen_name))
                            .collect::<Vec<_>>();

                        let img = format!(
                        "<a href=\"{}\"><img src=\"{}\" width=\"40px\" height=\"40px\" align=\"center\"/></a>",
                        user.profile_image_url_https, user.profile_image_url_https
                    );
                        let id_link = format!(
                            "<a href=\"https://twitter.com/intent/user?user_id={}\">{}</a>",
                            user.id, user.id
                        );
                        let screen_name_link = format!(
                            "<a href=\"https://twitter.com/{}\">{}</a>",
                            user.screen_name, user.screen_name
                        );

                        let created_at = user.created_at()?.format("%Y-%m-%d");
                        let suspension_date = suspension_observed.format("%Y-%m-%d");

                        let mut status = String::new();
                        if user.protected {
                            status.push('🔒');
                        }
                        if user.verified {
                            status.push_str("✔️");
                        }

                        /*println!(
                            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td align=\"center\">{}</td><td>{}</td></tr>",
                            img,
                            id_link,
                            screen_name_link,
                            created_at,
                            suspension_date,
                            status,
                            user.followers_count
                        );*/
                        println!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    img,
                    id_link,
                    screen_name_link,
                    created_at,
                    suspension_date,
                    user.followers_count
                );
                    } else {
                        log::warn!("Could not find profile for user {}", user_id);
                    }
                } else {
                    log::warn!("Could not find suspension date for user {}", user_id);
                }
            }

            println!(r#"</table>"#);
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
    #[error("Invalid ID")]
    InvalidId(String),
    #[error("Date format error")]
    ChronoParse(#[from] chrono::format::ParseError),
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
    FromIds,
}
