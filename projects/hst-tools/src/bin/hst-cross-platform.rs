use clap::Parser;
use hst_tw_db::ProfileDb;
use hst_tw_profiles::model::User;
use regex::Regex;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = hst_cli::init_logging(opts.verbose)?;

    let link_re =
        Regex::new(r"(https?://|\b)(:?www\.)?(?:t\.me|gab\.(com|ai)|parler\.com)/[\w_/@\-]+")
            .unwrap();

    match opts.command {
        Command::Extract { input } => {
            let reader = BufReader::new(File::open(input)?);
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_writer(std::io::stdout());

            for line in reader.lines() {
                let line = line?;
                let profile: User = serde_json::from_str(&line)?;

                let links = extract_links(&link_re, &profile);

                if links.len() > 0 {
                    let mut record = Vec::with_capacity(4);
                    let id = profile.id.to_string();
                    let snapshot = profile.snapshot.to_string();
                    record.push(id.as_str());
                    record.push(profile.screen_name.as_str());
                    record.push(snapshot.as_str());
                    record.extend(links);
                    writer.write_record(&record)?;
                }
            }
        }
    }

    Ok(())
}

fn extract_links<'r, 't>(link: &'r Regex, profile: &'t User) -> Vec<&'t str> {
    let mut links = vec![];

    if let Some(description) = &profile.description {
        for m in link.find_iter(description) {
            links.push(m.as_str());
        }
    }

    if let Some(url) = profile.expanded_url() {
        for m in link.find_iter(url) {
            links.push(m.as_str());
        }
    }

    links.sort();
    links.dedup();
    links
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
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
#[clap(name = "hst-cross-platform", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Extract {
        /// NDJSON input path
        #[clap(short, long)]
        input: String,
    },
}
