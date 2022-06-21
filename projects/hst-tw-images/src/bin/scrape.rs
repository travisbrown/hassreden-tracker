use clap::Parser;
use hst_tw_images::{Error, Image, Size, Store};
use reqwest::Url;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, Write};
use std::path::Path;

async fn download_image<P: AsRef<Path>>(
    client: &reqwest::Client,
    image: &Image,
    output: P,
) -> Result<(Url, bool), Error> {
    let path = output.as_ref().join(image.path());

    let url = image.url();

    let response = client.get(url).send().await?;
    let response_url = response.url().clone();
    let bytes = response.bytes().await?;

    if !bytes.is_empty() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(&parent)?;
        }

        let mut file = File::create(path)?;
        file.write_all(&bytes)?;

        Ok((response_url, false))
    } else {
        Ok((response_url, true))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    let _ = hst_cli::init_logging(opts.verbose);

    let store = Store::new(&opts.output);
    let mut keys = store.keys().collect::<Result<HashSet<_>, _>>()?;

    let client = reqwest::Client::new();

    let mut count = 0;
    let mut redirect_count = 0;
    let mut skipped_count = 0;
    let mut empty_count = 0;

    for line in std::io::stdin().lock().lines() {
        let line = line?;
        match line.parse::<Image>() {
            Ok(image) => {
                let key = image.key();

                if !keys.contains(&key) {
                    let image = if opts.upgrade {
                        image.with_size(Size::Square400)
                    } else {
                        image
                    };

                    let (response_url, is_empty) =
                        download_image(&client, &image, &opts.output).await?;
                    count += 1;

                    if Url::parse(&image.url())
                        .ok()
                        .filter(|url| *url == response_url)
                        .is_none()
                    {
                        log::warn!(
                            "Redirect: {}, {}",
                            image.url().as_str(),
                            response_url.as_str()
                        );
                        redirect_count += 1;
                    }

                    if is_empty {
                        empty_count += 1;
                        println!("{}", image);
                    }

                    keys.insert(key);
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
        "Total downloaded: {}; redirected: {}; empty: {}; skipped: {}",
        count,
        redirect_count,
        empty_count,
        skipped_count
    );

    Ok(())
}

#[derive(Parser)]
#[clap(name = "scrape", about, version, author)]
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
