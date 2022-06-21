use clap::Parser;
use serde_json::json;
use std::io::{BufRead, BufReader};

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = hst_cli::init_logging(opts.verbose)?;

    match opts.command {
        Command::Extract => {
            for line in BufReader::new(std::io::stdin()).lines() {
                let line = line?;
                let value = serde_json::from_str(&line)?;
                if let Some(user_info) = hst_tw_profiles::stream::extract_user_info(&value, false)?
                {
                    for user in user_info.users {
                        println!("{}", json!(user).to_string());
                    }

                    for partial_user in user_info.partial_users {
                        println!(
                            "{},{},{},{}",
                            partial_user.id,
                            partial_user.screen_name,
                            user_info.snapshot.timestamp(),
                            partial_user.name.unwrap_or_default(),
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Profile stream error")]
    ProfileStream(#[from] hst_tw_profiles::stream::Error),
    #[error("JSON encoding error")]
    Json(#[from] serde_json::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

#[derive(Debug, Parser)]
#[clap(name = "hst-stream", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Extract,
}
