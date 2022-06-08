use clap::Parser;
use hst_tw_db::ProfileDb;
use hst_tw_profiles::model::User;
use std::fs::File;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = hst_cli::init_logging(opts.verbose)?;
    let db = ProfileDb::open(opts.db, true)?;

    match opts.command {
        Command::Import { input } => {
            let file = File::open(input)?;
            let reader = hst_tw_profiles::avro::reader(file)?;

            for value in reader {
                let user = apache_avro::from_value::<User>(&value?)?;
                db.update(&user)?;
            }
        }
        Command::Lookup { id } => {
            let users = db.lookup(id)?;

            for (_, _, user) in users {
                println!("{}", serde_json::to_value(user)?);
            }
        }
        Command::Count => {
            let mut user_count = 0;
            let mut screen_name_count = 0;
            let mut verified = 0;
            let mut protected = 0;
            for result in db.iter() {
                let batch = result?;

                user_count += 1;
                screen_name_count += batch.len();

                if let Some((_, _, profile)) = batch.last() {
                    if profile.verified {
                        verified += 1;
                    }
                    if profile.protected {
                        protected += 1;
                    }
                }
            }

            println!("{} users, {} screen names", user_count, screen_name_count);
            println!("{} verified, {} protected", verified, protected);
        }
        Command::CountRaw => {
            let mut user_ids = std::collections::HashSet::new();
            let mut screen_name_count = 0;

            for result in db.raw_iter() {
                let (user_id, (_, _, _user)) = result?;

                user_ids.insert(user_id);
                screen_name_count += 1;
            }

            println!(
                "{} users, {} screen names",
                user_ids.len(),
                screen_name_count
            );
        }
        Command::Stats => {
            println!("Estimate the number of keys: {}", db.estimate_key_count()?);
            println!("{:?}", db.statistics());
        }
        Command::ScreenNames => {
            for result in db.iter() {
                let batch = result?;
                if let Some((_, _, most_recent)) = batch.last() {
                    println!("{},{}", most_recent.id, most_recent.screen_name);
                } else {
                    log::error!("Empty user result when reading database");
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
    #[error("Profile Avro error")]
    ProfileAvro(#[from] hst_tw_profiles::avro::Error),
    #[error("Avro decoding error")]
    Avro(#[from] apache_avro::Error),
    #[error("JSON encoding error")]
    Json(#[from] serde_json::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

#[derive(Debug, Parser)]
#[clap(name = "hst-profile-db", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Database directory path
    #[clap(long)]
    db: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Import {
        /// Avro input path
        #[clap(short, long)]
        input: String,
    },
    Lookup {
        /// Twitter user ID
        id: u64,
    },
    Count,
    CountRaw,
    Stats,
    ScreenNames,
}
