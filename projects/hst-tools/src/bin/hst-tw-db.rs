use hst_cli::prelude::*;
use hst_tw_db::{
    table::{ReadOnly, Table, Writeable},
    ProfileDb,
};
use hst_tw_profiles::model::User;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::Import { input } => {
            let db = ProfileDb::<Writeable>::open(opts.db, false)?;

            let reader = BufReader::new(zstd::stream::read::Decoder::new(File::open(input)?)?);

            for line in reader.lines() {
                let line = line?;
                let profile: User = serde_json::from_str(&line)?;
                db.update(&profile)?;
            }
        }
        Command::Lookup { id } => {
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;
            let users = db.lookup(id)?;

            for (_, user) in users {
                println!("{}", serde_json::to_value(user)?);
            }
        }
        Command::Count => {
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;
            let mut user_count = 0;
            let mut snapshot_count = 0;
            let mut screen_name_count = 0;
            let mut verified = 0;
            let mut protected = 0;
            for result in db.iter() {
                let (_, users) = result?;
                let mut screen_names = HashSet::new();

                user_count += 1;
                snapshot_count += users.len();

                for (_, user) in &users {
                    screen_names.insert(user.screen_name.clone());
                }

                if let Some((_, user)) = users.last() {
                    if user.verified {
                        verified += 1;
                    }
                    if user.protected {
                        protected += 1;
                    }
                }

                screen_name_count += screen_names.len();
            }

            println!(
                "{} users, {} screen names, {} snapshots",
                user_count, screen_name_count, snapshot_count
            );
            println!("{} verified, {} protected", verified, protected);
        }
        Command::Stats => {
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;
            if let Some(count) = db.get_estimated_key_count()? {
                println!("Estimated number of keys: {}", count);
            }
            println!("{:?}", db.statistics());
        }
        Command::Ids => {
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;
            for result in db.user_id_iter() {
                let (user_id, count, snapshot) = result?;
                println!("{},{},{}", user_id, count, snapshot.timestamp());
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
#[clap(name = "hst-tw-db", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
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
    Stats,
    Ids,
}
