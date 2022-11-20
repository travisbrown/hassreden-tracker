use chrono::{NaiveDate, Utc};
use hst_cli::prelude::*;
use hst_tw_db::{table::ReadOnly, ProfileDb};
use hst_tw_profiles::model::User;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("ProfileDb error")]
    ProfileDb(#[from] hst_tw_db::Error),
    #[error("Profile Avro error")]
    ProfileAvro(#[from] hst_tw_profiles::avro::Error),
    #[error("Avro decoding error")]
    Avro(#[from] apache_avro::Error),
    #[error("JSON encoding error")]
    Json(#[from] serde_json::Error),
    #[error("Deactivations file error")]
    Deactivations(#[from] hst_deactivations::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("CSV error")]
    Csv(#[from] csv::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let today = Utc::now().date_naive();
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::Report {
            db,
            deactivations,
            count,
            days,
        } => {
            let db = ProfileDb::<ReadOnly>::open(db, true)?;
            let deactivations =
                hst_deactivations::DeactivationLog::read(File::open(deactivations)?)?;
            let lines = std::io::stdin().lock().lines();
            let mut by_date =
                HashMap::<NaiveDate, Vec<(u64, Option<User>, Option<u32>, usize)>>::new();
            log::info!("Parsing stdin");

            for (ranking, line) in lines.enumerate() {
                let line = line?;
                let fields = line.split(',').collect::<Vec<_>>();
                let id = fields[0].parse::<u64>().unwrap();
                if ranking % 1000000 == 0 {
                    log::info!("Processing line {}", ranking);
                }

                if let Some(creation) = hst_tw_utils::snowflake_to_date_time(id as i64) {
                    let date = creation.date_naive();
                    //log::info!("{} {}", date, (today - date).num_days());

                    if (today - date).num_days() <= days.unwrap_or(usize::MAX) as i64 {
                        //log::info!("{}", id);
                        let for_date = by_date.entry(date).or_default();

                        if for_date.len() < count {
                            let user = db.lookup(id)?;
                            let status = deactivations.status(id);
                            for_date.push((
                                id,
                                user.first().map(|(_, user)| user).cloned(),
                                status,
                                ranking + 1,
                            ));
                        }
                    }
                }
            }

            let mut by_date = by_date.into_iter().collect::<Vec<_>>();
            by_date.sort_by_key(|(date, _)| std::cmp::Reverse(*date));

            for (date, users) in by_date.into_iter().take(days.unwrap_or(usize::MAX)) {
                println!("## {}", date.format("%e %B %Y"));
                println!();
                println!(
                    r#"<table><tr><th></th><th align="left">Twitter ID</th><th align="left">Screen name</th>"#
                );
                println!(
                    r#"<th align="left">Created</th><th align="left">Status</th><th align="left">Follower count</th><th align="left">Ranking</th></tr>"#
                );

                for (id, user, deactivation_status, ranking) in users {
                    if let Some(user) = user {
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

                        let created_at = user.created_at().unwrap().format("%Y-%m-%d");

                        let mut status = String::new();
                        if user.protected {
                            status.push('🔒');
                        }
                        if user.verified {
                            status.push_str("✔️");
                        }
                        if deactivation_status == Some(63) {
                            status.push('🚫');
                        } else if deactivation_status == Some(50) {
                            status.push('👋');
                        }

                        println!(
                            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td align=\"center\">{}</td><td>{}</td><td>{}</td></tr>",
                            img,
                            id_link,
                            screen_name_link,
                            created_at,
                            status,
                            user.followers_count,
                            ranking
                        );
                    } else {
                        let id_link = format!(
                            "<a href=\"https://twitter.com/intent/user?user_id={}\">{}</a>",
                            id, id
                        );
                        println!(
                            "<tr><td></td><td>{}</td><td></td><td></td><td align=\"center\"></td><td></td><td>{}</td></tr>",
                            id_link,
                            ranking
                        );
                    }
                }
                println!(r#"</table>"#);
                println!();
            }
        }
    }

    Ok(())
}

#[derive(Parser)]
#[clap(name = "hst-tw-respawns", about, version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Report {
        /// Database directory path
        #[clap(long)]
        db: String,
        /// Deactivation file path
        #[clap(long)]
        deactivations: String,
        /// Count
        #[clap(long, default_value = "50")]
        count: usize,
        /// Days to report
        #[clap(long)]
        days: Option<usize>,
    },
}
