use clap::Parser;
use flate2::{write::GzEncoder, Compression};
use hst_tw_images::{model::Size, Error, Image, Store};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufRead;
use std::path::PathBuf;

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = hst_cli::init_logging(opts.verbose);

    match opts.command {
        Command::Merge { from, into } => {
            let from_store = Store::new(from);
            let into_store = Store::new(into);

            let from_images = from_store
                .into_iter()
                .collect::<Result<HashMap<Image, PathBuf>, _>>()?;
            let into_images = into_store
                .into_iter()
                .collect::<Result<HashMap<Image, PathBuf>, _>>()?;

            for (image, from_path) in from_images {
                if let Some(into_path) = into_images.get(&image) {
                    let from_image_metadata = std::fs::metadata(from_path)?;
                    let into_image_metadata = std::fs::metadata(into_path)?;

                    log::info!(
                        "Collision: {} (size in source: {} bytes; size in target: {} bytes)",
                        image,
                        from_image_metadata.len(),
                        into_image_metadata.len()
                    );
                } else {
                    let into_path = into_store.path(image.path());

                    if let Some(parent) = into_path.parent() {
                        std::fs::create_dir_all(&parent)?;
                    }

                    std::fs::rename(from_path, into_path)?;
                }
            }
        }
        Command::Urls { base } => {
            let store = Store::new(base);

            for entry in &store {
                let (image, _) = entry?;
                println!("{}", image);
            }
        }
        Command::Paths { base } => {
            let store = Store::new(&base);

            for entry in &store {
                let (_, path) = entry?;
                println!("{}", path.strip_prefix(&base).unwrap().to_string_lossy());
            }
        }
        Command::Keys { output, base } => {
            let mut out = GzEncoder::new(File::create(output)?, Compression::default());
            let store = Store::new(base);

            store.write_keys(&mut out)?;
        }
        Command::FilterKnown { base } => {
            let store = Store::new(base);
            let keys = store.keys().collect::<Result<HashSet<_>, _>>()?;

            for line in std::io::stdin().lock().lines() {
                let line = line?;
                if line != "https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png" {
                    match line.trim().parse::<Image>() {
                        Ok(image) => {
                            if !keys.contains(&image.key()) {
                                println!("{}", image);
                            }
                        }
                        Err(error) => {
                            log::error!("{}: {:?}", line, error);
                        }
                    }
                }
            }
        }
        Command::ExtractPaths { base } => {
            let store = Store::new(base);

            for line in std::io::stdin().lock().lines() {
                let line = line?;
                let image = line.parse::<Image>()?.with_size(Size::Square400);
                let path = store.path(image.path());

                if path.exists() {
                    println!("{}", path.display());
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "store", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    /// Move the contents of one store into another
    Merge {
        #[clap(long)]
        from: String,
        #[clap(long)]
        into: String,
    },
    /// Dump a list of URLs as text
    Urls { base: String },
    /// Dump a list of paths as text
    Paths { base: String },
    /// Export gzipped key file
    Keys {
        #[clap(short, long)]
        output: String,
        base: String,
    },
    /// Filter known URLs from stdin
    FilterKnown { base: String },
    /// Return paths for a list of urls from stdin
    ExtractPaths { base: String },
}
