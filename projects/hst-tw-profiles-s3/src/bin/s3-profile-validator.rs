use hst_cli::prelude::*;
use hst_tw_profiles_s3::Config;
use sha2::{Digest as Sha2Digest, Sha512};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("S3 error")]
    S3(#[from] s3::error::S3Error),
    #[error("hst-tw-profiles-s3 error")]
    ProfilesS3(#[from] hst_tw_profiles_s3::Error),
    #[error("Config file error")]
    ConfigParse(#[from] toml::de::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Log initialization error")]
    LogInitialization(#[from] log::SetLoggerError),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    let config_str = std::fs::read_to_string(opts.config)?;
    let config = toml::from_str::<Config>(&config_str)?;
    let bucket = config.bucket()?;
    println!("{config:?}");

    use std::io::Read;

    let mut buffer = Vec::new();
    std::fs::File::open("test.txt")?.read_to_end(&mut buffer)?;

    let mut hasher = Sha512::new();
    hasher.update(&buffer);
    let result512 = hasher.finalize();

    println!("{}", hex::encode(result512));

    let len = buffer.len().to_le_bytes();

    for b in len {
        if b != 0 {
            buffer.push(b);
        }
    }

    let c = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
    println!("{} {}", buffer.len(), c.checksum(&buffer));

    /*let results = bucket.list(config.prefix, Some("/".to_string())).await?;

    for result in &results[0].contents {
        println!("* {:?}", result.key);
    }*/

    match opts.command {
        Command::Validate => {}
    }

    Ok(())
}

#[derive(Parser)]
#[clap(name = "s3-profile-validator", about, version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    /// Config file path
    #[clap(long, default_value = "aws.toml")]
    config: String,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    /// Validate
    Validate,
}
