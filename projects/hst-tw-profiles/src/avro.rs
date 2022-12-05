use super::model::User;
use apache_avro::{schema::Schema, Codec, Reader, Writer};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::{Read, Write};

pub fn writer<W: Write>(writer: W) -> Writer<'static, W> {
    Writer::with_codec(&USER_SCHEMA, writer, Codec::Snappy)
}

pub fn reader<R: Read>(reader: R) -> Result<Reader<'static, R>, apache_avro::Error> {
    Reader::with_schema(&USER_SCHEMA, reader)
}

pub fn validate<R: Read>(reader: Reader<'static, R>) -> Result<usize, ValidationError> {
    let mut count = 0;
    let mut last_snapshot = 0;
    let mut last_user_id = 0;
    let mut misordered_line_numbers = vec![];
    let mut duplicate_line_numbers = vec![];

    for (line_number, value) in reader.enumerate() {
        let user = apache_avro::from_value::<User>(&value?)?;

        match user.snapshot.cmp(&last_snapshot) {
            Ordering::Greater => {}
            Ordering::Less => {
                misordered_line_numbers.push(line_number);
            }
            Ordering::Equal => match user.id.cmp(&last_user_id) {
                Ordering::Greater => {}
                Ordering::Less => {
                    misordered_line_numbers.push(line_number);
                }
                Ordering::Equal => {
                    duplicate_line_numbers.push(line_number);
                }
            },
        }

        last_snapshot = user.snapshot;
        last_user_id = user.id;
        count += 1;
    }

    if misordered_line_numbers.is_empty() && duplicate_line_numbers.is_empty() {
        Ok(count)
    } else {
        Err(ValidationError::InvalidContents {
            misordered_line_numbers,
            duplicate_line_numbers,
        })
    }
}

pub fn count_users<R: Read>(
    reader: Reader<'static, R>,
) -> Result<HashMap<(u64, String), usize>, Error> {
    let mut counts = HashMap::new();

    for value in reader {
        let user = apache_avro::from_value::<User>(&value?)?;

        let count = counts.entry((user.id(), user.screen_name)).or_default();
        *count += 1;
    }

    Ok(counts)
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Avro error")]
    Avro(#[from] apache_avro::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    #[error("Avro error")]
    Avro(#[from] apache_avro::Error),
    #[error("Unsorted lines")]
    InvalidContents {
        misordered_line_numbers: Vec<usize>,
        duplicate_line_numbers: Vec<usize>,
    },
}

lazy_static::lazy_static! {
    pub static ref USER_SCHEMA: Schema = load_user_avro_schema().unwrap();
}

fn load_user_avro_schema() -> Result<Schema, Error> {
    let source = std::include_str!("../schemas/avro/user.avsc");

    Ok(Schema::parse_str(source)?)
}
