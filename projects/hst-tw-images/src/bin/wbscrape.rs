use clap::Parser;
use hst_tw_images::{Image, Store};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, Write};
use std::path::Path;
use wayback_rs::{cdx::IndexClient, Downloader, Item};

async fn download_image<P: AsRef<Path>>(
    downloader: &Downloader,
    image: &Image,
    item: &Item,
    output: P,
) -> Result<bool, Box<dyn std::error::Error>> {
    let path = output.as_ref().join(image.path());

    let bytes = downloader.download_item(item).await?;

    if !bytes.is_empty() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(&parent)?;
        }

        let mut file = File::create(path)?;
        file.write_all(&bytes)?;

        Ok(false)
    } else {
        Ok(true)
    }
}

fn select_item(items: &[Item]) -> Option<(Image, &Item)> {
    let valid = items
        .iter()
        .filter(|item| item.status != Some(404) && item.status != Some(302))
        .filter_map(|item| match item.url.parse::<Image>() {
            Ok(image) => Some((image, item)),
            Err(error) => {
                log::warn!("Can't parse URL for item ({:?}): {:?}", item, error);
                None
            }
        });

    valid.max_by_key(|(image, item)| (image.size, item.length))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    let _ = hst_tw_images::log::init(opts.verbose);

    let store = Store::new(&opts.output);
    let mut keys = store.keys().collect::<Result<HashSet<_>, _>>()?;

    let cdx = IndexClient::default();
    let downloader = Downloader::default();

    let mut count = 0;
    let mut skipped_count = 0;
    let mut empty_count = 0;

    for line in std::io::stdin().lock().lines() {
        let line = line?;
        match line.parse::<Image>() {
            Ok(image) => {
                let key = image.key();

                if !keys.contains(&key) {
                    let query = format!("{}*", image.id_prefix_url());
                    match cdx.search(&query, None, None).await {
                        Ok(items) => {
                            if let Some((image, item)) = select_item(&items) {
                                match download_image(&downloader, &image, item, &opts.output).await
                                {
                                    Ok(is_empty) => {
                                        count += 1;

                                        if is_empty {
                                            empty_count += 1;
                                            println!("{}", image);
                                        }

                                        keys.insert(key);
                                    }
                                    Err(error) => {
                                        log::error!("Download error: {:?}", error);
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            log::error!("Search error: {:?}", error);
                        }
                    }
                } else {
                    skipped_count += 1;
                }
            }
            Err(error) => {
                log::error!("Can't parse URL ({:?}): {}", error, line);
            }
        }
    }

    log::info!(
        "Total downloaded: {}; empty: {}; skipped: {}",
        count,
        empty_count,
        skipped_count
    );

    Ok(())
}

#[derive(Parser)]
#[clap(name = "wbscrape", about, version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Output directory
    #[clap(short, long)]
    output: String,
    /// Upgrade to highest-available quality
    #[clap(short, long)]
    upgrade: bool,
}
