use hst_cli::prelude::*;
use hst_tw_images::{Image, Store};
use reqwest::Url;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Twitter image error")]
    TwitterImage(#[from] hst_tw_images::Error),
    #[error("Twitter image store error")]
    TwitterImageStore(#[from] hst_tw_images::store::Error),
    #[error("HTTP client error")]
    HttpClient(#[from] reqwest::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

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
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::StoreUrls { base } => {
            let store = Store::new(base);

            for entry in &store {
                let (image, _) = entry?;
                println!("{}", image);
            }
        }
        Command::Scrape => todo!(),
    }

    Ok(())
}

#[derive(Parser)]
#[clap(name = "hst-tw-images", about, version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    /// Download
    Scrape,
    /// Dump a list of URLs (arbitrarily ordered) from a store as text
    StoreUrls { base: String },
}
