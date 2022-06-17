use csv::ReaderBuilder;
use std::fs::File;
use std::io::Read;
use wayback_rs::Item;

fn main() {
    let file = File::open("test.csv").unwrap();
    let input = read_csv(file).unwrap();

    hst_cdx::write_file(input.into_iter(), "test.parquet", 128).unwrap();
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
