use chrono::{DateTime, TimeZone, Utc};
use hst_cli::prelude::*;
use hst_deactivations::DeactivationLog;
use hst_tw_db::{table::ReadOnly, ProfileDb};
use itertools::Itertools;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let db = ProfileDb::<ReadOnly>::open(opts.db, true)?;
    let log = DeactivationLog::read(File::open(&opts.deactivations)?)?;

    let start = Utc.timestamp_opt(opts.start, 0).unwrap();
    let deactivations = read_deactivations(opts.deactivations, start)?;
    let other = read_other(opts.other)?;

    let mut combined = other
        .into_iter()
        .map(|(id, reversal)| (id, (None, reversal)))
        .collect::<HashMap<u64, (Option<DateTime<Utc>>, DateTime<Utc>)>>();

    for (id, (observed, reversal)) in deactivations {
        combined.insert(id, (Some(observed), reversal));
    }

    combined.retain(|_, (_, reversal)| *reversal >= start);

    let worst = read_worst(opts.worst)?;

    let users = match opts.users {
        Some(users) => {
            let reader = hst_tw_profiles::ProfileReader::open(users);

            reader
                .map(|user| user.map(|user| (user.id(), user)))
                .collect::<Result<HashMap<_, _>, _>>()?
        }
        None => {
            let mut users = HashMap::new();

            for id in combined.keys().cloned() {
                if let Some((_, user)) = db.lookup_latest(id)? {
                    users.insert(id, user);
                } else {
                    log::error!("Could not find user {}", id);
                }
            }

            users
        }
    };

    log::info!("{} users found", users.len());

    let mut profiles = users.iter().collect::<Vec<_>>();
    profiles.sort_by_key(|(_, user)| user.id);

    let mut profiles_file = File::create(opts.profiles)?;

    for (_, profile) in profiles {
        writeln!(profiles_file, "{}", serde_json::json!(profile))?;
    }

    let mut by_date = vec![];
    let mut combined_data = combined.into_iter().collect::<Vec<_>>();
    combined_data.sort_by_key(|(id, (_, reversal))| (reversal.date_naive(), *id));

    let mut data_file = File::create(opts.timestamps)?;

    for (id, (suspension, reversal)) in &combined_data {
        writeln!(
            data_file,
            "{},{},{}",
            id,
            suspension
                .map(|suspension| suspension.timestamp().to_string())
                .unwrap_or_default(),
            reversal.timestamp()
        )?;
    }

    for (date, group) in &combined_data
        .into_iter()
        .group_by(|(_, (_, reversal))| reversal.date_naive())
    {
        by_date.push((date, group.collect::<Vec<_>>()));
    }

    by_date.sort_by_key(|(date, _)| Reverse(*date));

    let mut report_file = File::create(opts.report)?;

    //writeln!(report_file, "# Elon Musk's suspension reversals")?;
    //writeln!(report_file, "The tables below show notable Twitter suspension reversals for each day since Elon Musk took over as owner and CEO.\n")?;
    //writeln!(report_file, "Note that only reversals of permanent suspensions are shown, not instances of accounts being unlocked after being restricted for rule violation.")?;
    //writeln!(report_file, "All dates indicate when the suspension or reversal was detected, and the actual suspension or reversal may have been earlier (these dates will get more precise as we refine the report).\n")?;
    //writeln!(report_file, "In some cases we do not have suspension detection dates (these are accounts that are known to have been suspended before February 2022).\n")?;

    writeln!(report_file, "## Table of contents")?;

    for (date, reversals) in &by_date {
        writeln!(
            report_file,
            "* [{}](#{}) ({})",
            date.format("%d %B %Y"),
            date.format("%d-%B-%Y"),
            reversals.len()
        )?;
    }

    for (date, reversals) in by_date {
        writeln!(report_file, "\n## {}", date.format("%d %B %Y"))?;

        let mut users = reversals
            .iter()
            .filter_map(|(id, (suspension, reversal))| {
                users.get(id).map(|user| {
                    (
                        id,
                        (
                            user,
                            suspension,
                            reversal,
                            worst.get(id).unwrap_or(&usize::MAX),
                        ),
                    )
                })
            })
            .collect::<Vec<_>>();

        writeln!(
            report_file,
            "Total suspension reversals observed: {}",
            users.len()
        )?;

        writeln!(report_file, "\n### By follower count")?;

        users.sort_by_key(|(id, (user, _, _, _))| (Reverse(user.followers_count), *id));

        let mut remaining = users.split_off(opts.size_count.min(users.len()));

        print_table(&mut report_file, &log, &users)?;

        if !remaining.is_empty() {
            writeln!(report_file, "\n### Other notable reversals")?;

            remaining.sort_by_key(|(id, (_, _, _, ranking))| (*ranking, *id));
            remaining.truncate(opts.other_count);

            print_table(&mut report_file, &log, &remaining)?;
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "report", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Database directory path
    #[clap(long)]
    db: String,
    /// Deactivations file
    #[clap(long)]
    deactivations: String,
    /// Other suspensions file
    #[clap(long)]
    other: String,
    /// Ranking file
    #[clap(long)]
    worst: String,
    /// Report output file
    #[clap(long, default_value = "report.md")]
    report: String,
    /// Timestamp output file
    #[clap(long, default_value = "timestamps.csv")]
    timestamps: String,
    /// Profiles output file
    #[clap(long, default_value = "profiles.ndjson")]
    profiles: String,
    /// Size count
    #[clap(long, default_value = "100")]
    size_count: usize,
    /// Other count
    #[clap(long, default_value = "100")]
    other_count: usize,
    /// User data file
    #[clap(long)]
    users: Option<String>,
    /// Beginning time stamp
    #[clap(long, default_value = "1666828800")]
    start: i64,
}

fn read_deactivations<P: AsRef<Path>>(
    path: P,
    start: DateTime<Utc>,
) -> Result<HashMap<u64, (DateTime<Utc>, DateTime<Utc>)>, Box<dyn std::error::Error>> {
    let reader = BufReader::new(File::open(path)?);
    let mut result = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        if line.contains(",63,") && !line.ends_with(',') {
            let parts = line.split(',').collect::<Vec<_>>();
            let id = parts[0].parse::<u64>()?;
            let observed = Utc
                .timestamp_opt(parts[2].parse::<i64>()?, 0)
                .single()
                .unwrap();
            let reversal = Utc
                .timestamp_opt(parts[3].parse::<i64>()?, 0)
                .single()
                .unwrap();

            if reversal >= start {
                result.insert(id, (observed, reversal));
            }
        }
    }

    Ok(result)
}

fn read_other<P: AsRef<Path>>(
    path: P,
) -> Result<HashMap<u64, DateTime<Utc>>, Box<dyn std::error::Error>> {
    let reader = BufReader::new(File::open(path)?);
    let mut result = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        let parts = line.split(',').collect::<Vec<_>>();
        let id = parts[0].parse::<u64>()?;
        let reversal = Utc
            .timestamp_opt(parts[3].parse::<i64>()?, 0)
            .single()
            .unwrap();

        result.insert(id, reversal);
    }

    Ok(result)
}

fn read_worst<P: AsRef<Path>>(path: P) -> Result<HashMap<u64, usize>, Box<dyn std::error::Error>> {
    let reader = BufReader::new(File::open(path)?);
    let mut result = HashMap::new();

    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        let parts = line.split(',').collect::<Vec<_>>();
        let id = parts[0].parse::<u64>()?;

        result.insert(id, i + 1);
    }

    Ok(result)
}

fn print_table<W: Write>(
    writer: &mut W,
    deactivations: &DeactivationLog,
    users: &[(
        &u64,
        (
            &hst_tw_profiles::model::User,
            &Option<DateTime<Utc>>,
            &DateTime<Utc>,
            &usize,
        ),
    )],
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        writer,
        r#"<table><tr><th></th><th align="left">Twitter ID</th><th align="left">Screen name</th>"#
    )?;
    writeln!(
        writer,
        r#"<th align="left">Created</th><th align="left">Status</th><th align="left">Suspended</th><th align="left">Followers</th>"#
    )?;

    for (id, (user, suspension, _, _)) in users {
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
        let suspension_observed = suspension
            .map(|suspension| suspension.format("%Y-%m-%d").to_string())
            .unwrap_or_default();

        let deactivation_status = deactivations.status(**id);

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

        writeln!(writer,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td align=\"center\">{}</td><td>{}</td><td>{}</td></tr>",
                img,
                id_link,
                screen_name_link,
                created_at,
                status,
                suspension_observed,
                user.followers_count
            )?;
    }
    writeln!(writer, "</table>")?;

    Ok(())
}
