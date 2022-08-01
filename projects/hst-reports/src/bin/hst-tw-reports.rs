use clap::Parser;
use hst_deactivations::{DeactivationLog, Entry};
use hst_tw_db::{table::ReadOnly, ProfileDb};
use hst_tw_profiles::model::User;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    hst_cli::init_logging(opts.verbose)?;
    let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;

    match opts.command {
        Command::SuspensionReport {
            deactivations,
            suspensions,
        } => {
            let log = DeactivationLog::read(File::open(deactivations)?)?;
            let suspended_user_ids = log.ever_deactivated(Some(63));

            let mut suspended_user_profiles: HashMap<u64, User> = HashMap::new();

            for user_id in &suspended_user_ids {
                let batch = db.lookup(*user_id)?;

                if let Some((_, _, most_recent)) = batch.last() {
                    if suspended_user_ids.contains(&most_recent.id()) {
                        suspended_user_profiles.insert(most_recent.id(), most_recent.clone());
                    }
                }
            }

            let suspension_report = File::create(suspensions)?;
            let not_found =
                write_suspension_report(suspension_report, &log, &suspended_user_profiles)?;

            log::info!(
                "Could not find profiles for {} suspended accounts",
                not_found
            );
        }
        Command::Reports {
            deactivations,
            suspensions,
            screen_name_changes,
        } => {
            let log = DeactivationLog::read(File::open(deactivations)?)?;
            let suspended_user_ids = log.ever_deactivated(Some(63));

            let mut suspended_user_profiles: HashMap<u64, User> = HashMap::new();
            let mut screen_name_change_user_profiles: HashMap<u64, Vec<_>> = HashMap::new();

            for result in db.iter() {
                let batch = result?;
                if let Some((_, _, most_recent)) = batch.last() {
                    if suspended_user_ids.contains(&most_recent.id()) {
                        suspended_user_profiles.insert(most_recent.id(), most_recent.clone());
                    }

                    if batch.len() > 1 {
                        screen_name_change_user_profiles.insert(most_recent.id(), batch);
                    }
                }
            }

            let suspension_report = File::create(suspensions)?;
            let not_found =
                write_suspension_report(suspension_report, &log, &suspended_user_profiles)?;

            log::info!(
                "Could not find profiles for {} suspended accounts",
                not_found
            );

            let mut screen_name_change_report = File::create(screen_name_changes)?;

            let mut user_id_vec = screen_name_change_user_profiles
                .into_iter()
                .collect::<Vec<_>>();
            user_id_vec.sort_by_key(|(id, _)| *id);

            for (_, profiles) in user_id_vec {
                let mut last_screen_name = "".to_string();

                for (first_timestamp, _, profile) in profiles {
                    if !last_screen_name.is_empty() {
                        writeln!(
                            screen_name_change_report,
                            "{},{},{},{},{},{},{},{}",
                            first_timestamp.timestamp(),
                            profile.id,
                            profile.verified,
                            profile.protected,
                            profile.followers_count,
                            last_screen_name,
                            profile.screen_name,
                            profile.profile_image_url_https
                        )?;
                    }
                    last_screen_name = profile.screen_name.clone();
                }
            }
        }
    }

    Ok(())
}

fn write_suspension_report<W: Write>(
    mut writer: W,
    log: &DeactivationLog,
    suspended_user_profiles: &HashMap<u64, User>,
) -> Result<usize, Error> {
    let mut not_found = 0;

    for (
        user_id,
        Entry {
            observed, reversal, ..
        },
    ) in log.deactivations(Some(63))
    {
        if let Some(profile) = suspended_user_profiles.get(&user_id) {
            writeln!(
                writer,
                "{},{},{},{},{},{},{},{},{},{}",
                observed.timestamp(),
                reversal
                    .map(|timestamp| timestamp.timestamp().to_string())
                    .unwrap_or_default(),
                profile.id(),
                profile
                    .created_at()
                    .map(|timestamp| timestamp.timestamp().to_string())
                    .unwrap_or_default(),
                profile.screen_name,
                profile.verified,
                profile.protected,
                profile.followers_count,
                profile.profile_image_url_https,
                profile.withheld_in_countries.join(";")
            )?;
        } else {
            writeln!(
                writer,
                "{},{},{},,,,,,,",
                observed.timestamp(),
                reversal
                    .map(|timestamp| timestamp.timestamp().to_string())
                    .unwrap_or_default(),
                user_id
            )?;
            not_found += 1;
        }
    }

    Ok(not_found)
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
    #[error("Deactivations file parsing error")]
    DeactivationsFile(#[from] hst_deactivations::Error),
}

#[derive(Debug, Parser)]
#[clap(name = "hst-tw-reports", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Database path
    #[clap(long)]
    db: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    SuspensionReport {
        /// Deactivations file path
        #[clap(long)]
        deactivations: String,
        /// Suspension report path
        #[clap(long, default_value = "suspensions.csv")]
        suspensions: String,
    },
    Reports {
        /// Deactivations file path
        #[clap(long)]
        deactivations: String,
        /// Suspension report path
        #[clap(long, default_value = "suspensions.csv")]
        suspensions: String,
        /// Screen name change report path
        #[clap(long, default_value = "screen-names.csv")]
        screen_name_changes: String,
    },
}
