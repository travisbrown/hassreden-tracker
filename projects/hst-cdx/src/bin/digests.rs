use chrono::{DateTime, Utc};
use csv::ReaderBuilder;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use wayback_rs::Item;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let db = hst_cdx::db::DigestDb::open(&args[1], true)?;

    let first = args.get(3).map(|path| Path::new(path));

    let mut paths = std::fs::read_dir(&args[2])?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    paths.sort();

    for path in paths {
        if first.map(|first| first <= path).unwrap_or(true) {
            //let entry = entry?;
            println!("Reading: {:?}", path);
            let file = File::open(path)?;
            let input = read_csv(file)?;

            for item in input {
                db.insert(
                    &item.digest,
                    &item.url,
                    DateTime::<Utc>::from_utc(item.archived_at, Utc),
                )?;
            }
        }
    }

    Ok(())
}

fn read_csv<R: Read>(reader: R) -> Result<Vec<Item>, Box<dyn std::error::Error>> {
    let mut csv_reader = ReaderBuilder::new().has_headers(false).from_reader(reader);

    csv_reader
        .records()
        .map(|record| {
            let row = record?;
            Ok(Item::parse_optional_record(
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
            )?)
        })
        .collect()
}
