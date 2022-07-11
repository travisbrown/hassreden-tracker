use arrow::csv::{Reader, ReaderBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::*;
use std::io::{Read, Seek};
use std::sync::Arc;

pub mod db;

const TIMESTAMP_FMT: &'static str = "%Y%m%d%H%M%S";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Arrow error")]
    Arrow(#[from] arrow::error::ArrowError),
}

lazy_static::lazy_static! {
    pub static ref CDX_CSV_SCHEMA: Arc<Schema> = Arc::new(
        Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            //Field::new("archived_at", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("archived_at", DataType::Utf8, false),
            Field::new("digest", DataType::Utf8, false),
            Field::new("mime_type", DataType::Utf8, false),
            Field::new("length", DataType::UInt32, false),
            Field::new("status", DataType::Utf8, false),
        ])
    );

    pub static ref CDX_CSV_SCHEMA_2: Schema =
        Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            //Field::new("archived_at", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("archived_at", DataType::Utf8, false),
            Field::new("digest", DataType::Utf8, false),
            Field::new("mime_type", DataType::Utf8, false),
            Field::new("length", DataType::UInt32, false),
            Field::new("status", DataType::Utf8, false),
        ])
    ;
}

pub fn open_csv_reader<R: Read + Seek>(reader: R) -> Result<Reader<R>, Error> {
    ReaderBuilder::new()
        .with_schema(CDX_CSV_SCHEMA.clone())
        .has_header(false)
        .with_datetime_format(TIMESTAMP_FMT.to_string())
        .build(reader)
        .map_err(Error::from)
}

pub fn csv_options() -> CsvReadOptions<'static> {
    CsvReadOptions::new()
        .has_header(false)
        .schema(&*CDX_CSV_SCHEMA_2)
}

/*use chrono::{DateTime, Utc};
use parquet::{
    basic::Compression,
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::SerializedFileWriter,
    },
    schema::{parser::parse_message_type, types::Type},
};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use wayback_rs::Item;


const CDX_SCHEMA_TEXT: &str = "
   message cdx {
        REQUIRED BINARY url (UTF8);
        REQUIRED INT64 archived_at;
        REQUIRED BINARY digest (UTF8);
        REQUIRED BINARY mime_type (UTF8);
        REQUIRED INT32 length;
        REQUIRED INT32 status;
    }
";

lazy_static::lazy_static! {
    pub static ref CDX_SCHEMA: Arc<Type> =
        Arc::new(parse_message_type(CDX_SCHEMA_TEXT).unwrap());
}

pub fn write_file<I: Iterator<Item = Item>, P: AsRef<Path>>(
    mut input: I,
    output: P,
    row_group_size: usize,
) -> Result<(), Error> {
    let schema = Arc::new(parse_message_type(CDX_SCHEMA_TEXT).unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD)
            .build(),
    );
    let file = File::create(output)?;
    let mut writer = SerializedFileWriter::new(file, schema, props)?;

    let mut row_group_items = Vec::with_capacity(row_group_size);
    let mut finished = false;

    while let Some(next) = input.next() {
        if finished {
            break;
        }

        let mut i = 1;
        row_group_items.clear();
        row_group_items.push(next);

        while i < row_group_size {
            if let Some(next) = input.next() {
                row_group_items.push(next);
                i += 1;
            } else {
                finished = true;
                break;
            }
        }

        let mut row_group_writer = writer.next_row_group()?;

        let mut column_writer = row_group_writer.next_column()?.unwrap();
        if let ColumnWriter::ByteArrayColumnWriter(ref mut column_writer) = column_writer {
            let values = row_group_items
                .iter()
                .map(|item| ByteArray::from(item.url.as_str()))
                .collect::<Vec<_>>();

            column_writer.write_batch(&values, None, None)?;
            //column_writer.close()?;
        } else {
            panic!("Invalid schema");
        }
        row_group_writer.close_column(column_writer)?;

        let mut column_writer = row_group_writer.next_column()?.unwrap();
        if let ColumnWriter::Int64ColumnWriter(ref mut column_writer) = column_writer {
            let values = row_group_items
                .iter()
                .map(|item| DateTime::<Utc>::from_utc(item.archived_at, Utc).timestamp())
                .collect::<Vec<_>>();

            column_writer.write_batch(&values, None, None)?;
            //column_writer.close()?;
        } else {
            panic!("Invalid schema");
        }
        row_group_writer.close_column(column_writer)?;

        let mut column_writer = row_group_writer.next_column()?.unwrap();
        if let ColumnWriter::ByteArrayColumnWriter(ref mut column_writer) = column_writer {
            let values = row_group_items
                .iter()
                .map(|item| ByteArray::from(item.digest.as_str()))
                .collect::<Vec<_>>();

            column_writer.write_batch(&values, None, None)?;
            //column_writer.close()?;
        } else {
            panic!("Invalid schema");
        }
        row_group_writer.close_column(column_writer)?;

        let mut column_writer = row_group_writer.next_column()?.unwrap();
        if let ColumnWriter::ByteArrayColumnWriter(ref mut column_writer) = column_writer {
            let values = row_group_items
                .iter()
                .map(|item| ByteArray::from(item.mime_type.as_str()))
                .collect::<Vec<_>>();

            column_writer.write_batch(&values, None, None)?;
            //column_writer.close()?;
        } else {
            panic!("Invalid schema");
        }
        row_group_writer.close_column(column_writer)?;

        let mut column_writer = row_group_writer.next_column()?.unwrap();
        if let ColumnWriter::Int32ColumnWriter(ref mut column_writer) = column_writer {
            let values = row_group_items
                .iter()
                .map(|item| item.length as i32)
                .collect::<Vec<_>>();

            column_writer.write_batch(&values, None, None)?;
            //column_writer.close()?;
        } else {
            panic!("Invalid schema");
        }
        row_group_writer.close_column(column_writer)?;

        let mut column_writer = row_group_writer.next_column()?.unwrap();
        if let ColumnWriter::Int32ColumnWriter(ref mut column_writer) = column_writer {
            let values = row_group_items
                .iter()
                .map(|item| item.status.map(|value| value as i32).unwrap_or(-1))
                .collect::<Vec<_>>();

            column_writer.write_batch(&values, None, None)?;
            //column_writer.close()?;
        } else {
            panic!("Invalid schema");
        }
        row_group_writer.close_column(column_writer)?;

        row_group_writer.close()?;
        writer.close_row_group(row_group_writer)?;
    }

    writer.close()?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Parquet error")]
    Parquet(#[from] parquet::errors::ParquetError),
}
*/
