use clap::Parser;
use flate2::read::GzDecoder;
use serde_json::json;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

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
        Command::ExtractDir { path, start } => {
            let mut paths = std::fs::read_dir(&path)?
                .map(|entry| Ok(entry?.path()))
                .collect::<Result<Vec<_>, std::io::Error>>()?;
            paths.sort();
            log::info!("Read {} paths", paths.len());

            for path in paths {
                let file = File::open(&path)?;
                let file_name = path
                    .file_name()
                    .and_then(|file_name| file_name.to_str())
                    .unwrap_or_default();

                if start
                    .as_ref()
                    .map(|start| file_name >= start.as_str())
                    .unwrap_or(true)
                {
                    log::info!("Reading {}", file_name);
                    let gz = GzDecoder::new(file);
                    for line in BufReader::new(gz).lines() {
                        match line {
                            Ok(line) => {
                                match serde_json::from_str(&line).map_err(Error::from).and_then(
                                    |value| {
                                        hst_tw_profiles::stream::extract_user_info(&value, false)
                                            .map_err(Error::from)
                                    },
                                ) {
                                    Ok(Some(user_info)) => {
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
                                    Ok(None) => {
                                        log::warn!("Empty item: {}", file_name);
                                    }

                                    Err(error) => {
                                        log::error!("{}", error);
                                    }
                                }
                            }
                            Err(error) => {
                                log::error!("{}", error);
                            }
                        }
                    }
                }
            }
        }
        Command::TarArchive { path, out } => {
            let out_path = Path::new(&out);
            std::fs::create_dir_all(out_path)?;

            let mut user_writer = BufWriter::new(File::create(out_path.join("profiles.ndjson"))?);

            let mut partial_user_writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_path(out_path.join("names.csv"))?;

            hst_tw_profiles::archive::extract_tar(path, |user_info| {
                match user_info {
                    Ok(Some(user_info)) => {
                        for user in user_info.users {
                            writeln!(user_writer, "{}", json!(user).to_string())?;
                        }

                        for partial_user in user_info.partial_users {
                            partial_user_writer
                                .write_record(&[
                                    partial_user.id.to_string(),
                                    partial_user.screen_name,
                                    user_info.snapshot.timestamp().to_string(),
                                    partial_user.name.unwrap_or_default(),
                                ])
                                .map_err(|_| {
                                    hst_tw_profiles::archive::Error::Other("CSV error".to_string())
                                })?;
                        }
                    }
                    Err(error) => {
                        log::error!("{:?}", error);
                    }
                    Ok(None) => {}
                }

                Ok(())
            })?;
        }
        Command::ZipArchive { path, out } => {
            let out_path = Path::new(&out);
            std::fs::create_dir_all(out_path)?;

            let mut user_writer = BufWriter::new(File::create(out_path.join("profiles.ndjson"))?);

            let mut partial_user_writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_path(out_path.join("names.csv"))?;

            hst_tw_profiles::archive::extract_zip(path, |user_info| {
                match user_info {
                    Ok(Some(user_info)) => {
                        for user in user_info.users {
                            writeln!(user_writer, "{}", json!(user).to_string())?;
                        }

                        for partial_user in user_info.partial_users {
                            partial_user_writer
                                .write_record(&[
                                    partial_user.id.to_string(),
                                    partial_user.screen_name,
                                    user_info.snapshot.timestamp().to_string(),
                                    partial_user.name.unwrap_or_default(),
                                ])
                                .map_err(|_| {
                                    hst_tw_profiles::archive::Error::Other("CSV error".to_string())
                                })?;
                        }
                    }
                    Err(error) => {
                        log::error!("{:?}", error);
                    }
                    Ok(None) => {}
                }

                Ok(())
            })?;
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Profile stream error")]
    ProfileStream(#[from] hst_tw_profiles::stream::Error),
    #[error("Profile archive error")]
    ProfileArchive(#[from] hst_tw_profiles::archive::Error),
    #[error("JSON encoding error")]
    Json(#[from] serde_json::Error),
    #[error("CSV error")]
    Csv(#[from] csv::Error),
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
    ExtractDir {
        /// Directory path
        path: String,
        /// Start digest
        #[clap(long)]
        start: Option<String>,
    },
    TarArchive {
        /// File path
        #[clap(long)]
        path: String,
        /// Output directory
        #[clap(long)]
        out: String,
    },
    ZipArchive {
        /// File path
        #[clap(long)]
        path: String,
        /// Output directory
        #[clap(long)]
        out: String,
    },
}
