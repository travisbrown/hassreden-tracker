use chrono::{DateTime, TimeZone, Utc};
use csv::ReaderBuilder;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use wayback_rs::Item;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();

    let mut paths = std::fs::read_dir(&args[1])?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    paths.sort();

    for path in paths {
        //println!("Reading: {:?}", path);
        let file = File::open(path)?;
        let input = read_timestamps(file)?;

        for timestamp in input {
            let ndt = wayback_rs::util::parse_timestamp(&timestamp).unwrap();
            let utc_s = DateTime::<Utc>::from_utc(ndt, Utc).timestamp();
            let utc_dt = Utc.timestamp(utc_s, 0);
            let utc_ndt = utc_dt.naive_utc();
            let timestamp_out = wayback_rs::util::to_timestamp(&utc_ndt);

            println!(
                "{},{},{}",
                if timestamp == timestamp_out { "1" } else { "0" },
                timestamp,
                timestamp_out
            );
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid row")]
    InvalidRow,
}

fn read_timestamps<R: Read>(reader: R) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut csv_reader = ReaderBuilder::new().has_headers(false).from_reader(reader);

    csv_reader
        .records()
        .map(|record| {
            let row = record?;
            Ok(row.get(1).ok_or(Error::InvalidRow)?.to_string())
        })
        .collect()
}
