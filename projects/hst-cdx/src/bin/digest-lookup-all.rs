use chrono::{DateTime, Utc};
use csv::ReaderBuilder;
use itertools::Itertools;
use std::fs::File;
use std::io::{BufRead, Read};
use wayback_rs::Item;

///const CHUNK_SIZE: usize = 5000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let db = hst_cdx::db::DigestDb::open(&args[1], true)?;
    //let mut lines = Vec::with_capacity(CHUNK_SIZE);
    //let mut digests = Vec::with_capacity(CHUNK_SIZE);

    for line in std::io::stdin().lock().lines() {
        //}.chunks(CHUNK_SIZE) {
        let line = line?;
        let parts = line.split(',').collect::<Vec<_>>();

        print!("{},", line);
        if let Some((url, timestamp)) = db.lookup(&parts[0])? {
            println!("{},{}", url, timestamp.timestamp());
        } else {
            println!();
        }
        /*lines.clear();
        digests.clear();
        for line in chunk {
            let line = line?;
            lines.push(line.clone());
            let parts = line.split(',').collect::<Vec<_>>();
            digests.push(parts[0].to_string());
        }

        let results = db.lookup_batch(&digests)?;

        for (line, result) in lines.iter().zip(results) {
            print!("{},", line);
            if let Some((url, timestamp)) = result {
                println!("{},{}", url, timestamp.timestamp());
            } else {
                println!();
            }
        }*/
    }

    Ok(())
}
