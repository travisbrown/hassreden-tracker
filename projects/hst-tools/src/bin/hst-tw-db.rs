use chrono::{DateTime, TimeZone, Utc};
use hst_cli::prelude::*;
use hst_deactivations::DeactivationLog;
use hst_tw_db::{
    table::{ReadOnly, Table, Writeable},
    ProfileDb,
};
use hst_tw_profiles::model::User;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::Import { input } => {
            let db = ProfileDb::<Writeable>::open(opts.db, false)?;

            if input.ends_with("zst") {
                let reader = BufReader::new(zstd::stream::read::Decoder::new(File::open(input)?)?);

                for line in reader.lines() {
                    let line = line?;
                    let profile: User = serde_json::from_str(&line)?;
                    db.update(&profile)?;
                }
            } else {
                let reader = BufReader::new(File::open(input)?);

                for line in reader.lines() {
                    let line = line?;
                    let profile: User = serde_json::from_str(&line)?;
                    db.update(&profile)?;
                }
            }
        }
        Command::Lookup { id } => {
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;
            let users = db.lookup(id)?;

            for (_, user) in users {
                println!("{}", serde_json::to_value(user)?);
            }
        }
        Command::LookupAll => {
            let lines = std::io::stdin().lock().lines();
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;

            for line in lines {
                let line = line?;
                let id = line.parse::<u64>().unwrap();
                let users = db.lookup(id)?;
                if let Some((_, user)) = users.first() {
                    println!("{},{},{}", user.id, user.screen_name, user.followers_count);
                } else {
                    println!("{},,", id);
                }
            }
        }
        Command::LookupAllJson { earliest } => {
            let lines = std::io::stdin().lock().lines();
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;

            for line in lines {
                let line = line?;
                let id = line.parse::<u64>().unwrap();
                let users = db.lookup(id)?;
                if let Some((_, user)) = if earliest {
                    users.last()
                } else {
                    users.first()
                } {
                    println!("{}", serde_json::json!(user));
                }
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
        Command::DisplayNameSearch { query } => {
            let query = query.to_lowercase();
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;
            let mut writer = csv::WriterBuilder::new().from_writer(std::io::stdout());

            for result in db.iter() {
                let (_, mut users) = result?;
                users.reverse();

                let mut is_match = false;

                if let Some((observed, user)) = users.first() {
                    let observed = observed.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    if user.name.to_lowercase().contains(&query) {
                        writer.write_record([
                            user.id.to_string(),
                            user.screen_name.to_string(),
                            user.followers_count.to_string(),
                            "first".to_string(),
                            observed.to_string(),
                            "".to_string(),
                            user.name.to_string(),
                        ])?;
                        is_match = true;
                    }
                }

                for pair in users.windows(2) {
                    let (_, previous_user) = &pair[0];
                    let (observed, user) = &pair[1];
                    let observed = observed.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

                    if !is_match && user.name.to_lowercase().contains(&query) {
                        writer.write_record([
                            user.id.to_string(),
                            user.screen_name.to_string(),
                            user.followers_count.to_string(),
                            "added".to_string(),
                            observed.to_string(),
                            previous_user.name.to_string(),
                            user.name.to_string(),
                        ])?;
                        is_match = true;
                    } else if is_match && !user.name.to_lowercase().contains(&query) {
                        writer.write_record([
                            user.id.to_string(),
                            user.screen_name.to_string(),
                            user.followers_count.to_string(),
                            "removed".to_string(),
                            observed.to_string(),
                            previous_user.name.to_string(),
                            user.name.to_string(),
                        ])?;
                        is_match = false;
                    }
                }
            }
        }
        Command::CheckReversals { deactivations } => {
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;
            let mut deactivations = DeactivationLog::read(File::open(deactivations)?)?;
            let mut deactivated_ids = deactivations
                .current_deactivated(None)
                .into_iter()
                .collect::<Vec<_>>();
            deactivated_ids.sort();
            let mut reversals = vec![];

            for id in deactivated_ids {
                let users = db.lookup(id)?;
                if let Some(deactivation_time) = deactivations.status_timestamp(id) {
                    if let Some((snapshot, user)) = users
                        .iter()
                        .rev()
                        .find(|(snapshot, _)| *snapshot > deactivation_time)
                    {
                        log::info!("{},{}", user.id, snapshot);
                        reversals.push((user.id(), *snapshot));
                    }
                }
            }

            if let Err(update_errors) = deactivations.update_with_reversals(reversals.into_iter()) {
                log::error!("{} update errors", update_errors.len());
            }
            deactivations.write(std::io::stdout())?;
        }
        Command::CheckOldReversals {
            candidates,
            tweets,
            beginning,
        } => {
            let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;

            // When old suspensions were last updated (2021-11-19).
            let last_update = Utc.timestamp_opt(1637280000, 0).unwrap();
            let beginning = match beginning {
                Some(beginning) => Utc
                    .timestamp_opt(beginning, 0)
                    .single()
                    .ok_or_else(|| Error::Format(beginning.to_string()))?,
                None => last_update,
            };

            let candidates_reader = BufReader::new(File::open(candidates)?);
            let mut candidates = candidates_reader
                .lines()
                .map(|line| {
                    let line = line.map_err(Error::from)?;
                    line.parse::<u64>().map_err(|_| Error::Format(line))
                })
                .collect::<Result<Vec<_>, _>>()?;
            candidates.sort_unstable();

            let tweet_timestamps = tweet_timestamps(tweets, Some(last_update))?;

            for id in candidates {
                let users = db.lookup(id)?;
                // The users are in reverse order.
                if let Some(((first_timestamp, _), (_, last_snapshot))) =
                    users.last().zip(users.first())
                {
                    let first_tweet_timestamp = tweet_timestamps
                        .get(&id)
                        .and_then(|timestamps| timestamps.first())
                        .filter(|timestamp| *timestamp < first_timestamp);

                    let reversal_observed = first_tweet_timestamp.unwrap_or(first_timestamp);

                    if *reversal_observed > beginning {
                        println!(
                            "{},{},{},{}",
                            last_snapshot.id,
                            last_snapshot.screen_name,
                            last_snapshot.followers_count,
                            reversal_observed.timestamp()
                        );
                    }
                }
            }
        }
        Command::CheckTweets {
            deactivations,
            tweets,
        } => {
            let mut deactivations = DeactivationLog::read(File::open(deactivations)?)?;
            let mut deactivated_ids = deactivations
                .ever_deactivated(None)
                .into_iter()
                .collect::<Vec<_>>();
            deactivated_ids.sort();
            let mut reversals = vec![];

            let tweet_timestamps = tweet_timestamps(tweets, None)?;

            for id in deactivated_ids {
                if let Some(entries) = deactivations.lookup(id) {
                    if let Some(timestamps) = tweet_timestamps.get(&id) {
                        for entry in entries {
                            //if entry.status == 63 {
                            if let Some(reversal) = entry.reversal {
                                for timestamp in timestamps {
                                    if *timestamp > entry.observed && *timestamp < reversal {
                                        log::info!("{}", timestamp);
                                        reversals.push((id, *timestamp));
                                        break;
                                    }
                                }
                            }
                            //}
                        }
                    }
                }
            }

            if let Err(update_errors) = deactivations.update_with_reversals(reversals.into_iter()) {
                log::error!("{} update errors", update_errors.len());
            }
            deactivations.write(std::io::stdout())?;
        }
        Command::Validate { deactivations } => {
            let deactivations = DeactivationLog::read(File::open(deactivations)?)?;
            if let Err(errors) = deactivations.validate() {
                for error in errors {
                    println!("{}", error);
                }
            }
        }
        Command::ValidateDup { deactivations } => {
            let deactivations = DeactivationLog::read(File::open(deactivations)?)?;
            if let Err(errors) = deactivations.validate() {
                for error in errors {
                    if let Some(entries) = deactivations.lookup(error) {
                        let revs = entries
                            .iter()
                            .filter_map(|entry| entry.reversal)
                            .collect::<Vec<_>>();
                        let uniq = revs.iter().collect::<HashSet<_>>();
                        if revs.len() > uniq.len() {
                            println!("{}", error);
                        }
                    }
                }
            }
        }
        Command::Fix { deactivations } => {
            let mut deactivations = DeactivationLog::read(File::open(deactivations)?)?;
            deactivations.fix();
            deactivations.write(std::io::stdout())?;
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Deactivations file error")]
    Deactivations(#[from] hst_deactivations::Error),
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
    #[error("CSV error")]
    Csv(#[from] csv::Error),
    #[error("Format error")]
    Format(String),
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
    LookupAll,
    LookupAllJson {
        /// Get earliest snapshot (default is latest)
        #[clap(long)]
        earliest: bool,
    },
    Count,
    Stats,
    Ids,
    DisplayNameSearch {
        query: String,
    },
    CheckReversals {
        /// Deactivations file input path
        #[clap(short, long)]
        deactivations: String,
    },
    CheckOldReversals {
        /// Candidates input path (one ID per line)
        #[clap(long)]
        candidates: String,
        /// Tweets input path (user ID, status ID, timestamp)
        #[clap(long)]
        tweets: String,
        /// Timestamp to begin filtering at
        #[clap(long)]
        beginning: Option<i64>,
    },
    CheckTweets {
        /// Deactivations file input path
        #[clap(short, long)]
        deactivations: String,
        /// Tweets input path (user ID, status ID, timestamp)
        #[clap(long)]
        tweets: String,
    },
    Validate {
        /// Deactivations file input path
        #[clap(short, long)]
        deactivations: String,
    },
    ValidateDup {
        /// Deactivations file input path
        #[clap(short, long)]
        deactivations: String,
    },
    Fix {
        /// Deactivations file input path
        #[clap(short, long)]
        deactivations: String,
    },
}

fn tweet_timestamps<P: AsRef<Path>>(
    path: P,
    last_update: Option<DateTime<Utc>>,
) -> Result<HashMap<u64, Vec<DateTime<Utc>>>, Error> {
    let tweets_reader = BufReader::new(File::open(path)?);
    let tweets = tweets_reader
        .lines()
        .map(|line| {
            let line = line.map_err(Error::from)?;
            let parts = line.split(',').collect::<Vec<_>>();
            let user_id = parts[0]
                .parse::<u64>()
                .map_err(|_| Error::Format(line.to_string()))?;
            let timestamp = parts[2]
                .parse::<i64>()
                .map_err(|_| Error::Format(line.to_string()))?;
            let timestamp = Utc
                .timestamp_opt(timestamp, 0)
                .single()
                .ok_or_else(|| Error::Format(line.to_string()))?;

            Ok::<_, Error>((user_id, timestamp))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut tweet_timestamps = HashMap::new();
    for (user_id, timestamps) in &tweets.into_iter().group_by(|(user_id, _)| *user_id) {
        let mut timestamps = timestamps
            .map(|(_, timestamp)| timestamp)
            .collect::<Vec<_>>();
        if let Some(last_update) = last_update {
            timestamps.retain(|timestamp| *timestamp > last_update);
        }
        timestamps.sort_unstable();
        tweet_timestamps.insert(user_id, timestamps);
    }

    Ok(tweet_timestamps)
}
