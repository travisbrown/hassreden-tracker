use csv::ReaderBuilder;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use wayback_rs::Item;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let known_redirect_digests = read_known_redirect_digests(&args[1])?;
    eprintln!("Read known");
    //let mut writer = csv::WriterBuilder::new().from_writer(std::io::stdout());

    let mut results = HashMap::new();

    let mut paths = std::fs::read_dir(&args[2])?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    paths.sort();

    for path in paths {
        eprintln!("Reading: {:?}", path);
        let file = File::open(path)?;
        let input = read_csv(file)?;

        for item in input {
            if item.status == Some(302) && !known_redirect_digests.contains(&item.digest) {
                let entry = results
                    .entry(item.digest.clone())
                    .or_insert_with(|| (0isize, (item.url.clone(), item.timestamp())));
                entry.0 += 1;
                //writer.write_record(item.to_record())?;
            }
        }
    }

    let mut output = results
        .into_iter()
        .map(|(digest, (count, (url, timestamp)))| (count, digest, url, timestamp))
        .collect::<Vec<_>>();
    output.sort_by_key(|(count, _, _, _)| -count);

    for (count, digest, url, timestamp) in output {
        println!("{},{},{},{}", digest, url, timestamp, count);
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

fn read_known_redirect_digests<P: AsRef<Path>>(
    directory: P,
) -> Result<HashSet<String>, std::io::Error> {
    let mut paths = std::fs::read_dir(directory)?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    paths.sort();

    let mut digests = HashSet::new();

    for path in paths {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if let Some(digest) = line.split(',').next() {
                digests.insert(digest.to_string());
            }
        }
    }

    Ok(digests)
}
