use chrono::{DateTime, Utc};
use csv::ReaderBuilder;
use std::fs::File;
use std::io::Read;
use wayback_rs::Item;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let db = hst_cdx::db::DigestDb::open(&args[1], true)?;
    for digest in args[2].split(",") {
        print!("{},", digest);
        if let Some((url, timestamp)) = db.lookup(digest)? {
            println!("{},{}", url, timestamp.timestamp());
        }
    }
    Ok(())
}
