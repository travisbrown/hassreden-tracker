use chrono::{DateTime, NaiveDateTime, Utc};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::logical_plan::{DFSchema, ExprSchemable};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

const TIMESTAMP_FMT: &str = "%Y%m%d%H%M%S";

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    use arrow::array::Array;
    let args = std::env::args().collect::<Vec<_>>();

    // register the table
    let ctx = SessionContext::new();
    //ctx.register_csv("example", &args[1], hst_cdx::csv_options()).await?;
    // create a plan to run a SQL query
    //let df = ctx.sql("SELECT COUNT(*) FROM example").await?;
    // execute and print results
    //result.show().await?;
    let df = ctx.read_csv(&args[1], hst_cdx::csv_options()).await?;

    let parse_timestamp = create_udf(
        "parse_timestamp",
        vec![DataType::Utf8],
        Arc::new(DataType::Timestamp(TimeUnit::Second, None)),
        Volatility::Immutable,
        Arc::new(|values| {
            let result = match &values[0] {
                ColumnarValue::Scalar(value) => match value {
                    ScalarValue::Utf8(Some(value)) => {
                        ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                            Some(
                                DateTime::<Utc>::from_utc(
                                    NaiveDateTime::parse_from_str(value, TIMESTAMP_FMT).map_err(
                                        |error| DataFusionError::External(Box::new(error)),
                                    )?,
                                    Utc,
                                )
                                .timestamp(),
                            ),
                            None,
                        ))
                    }
                    _other => panic!("unknown value"),
                },
                ColumnarValue::Array(values) => {
                    let strings = arrow::array::as_string_array(values);
                    let mut builder = arrow::array::TimestampSecondBuilder::new(strings.len());
                    //let result = arrow::array::ArrayData::new_empty(DataType::Timestamp(TimeUnit::Second, None));

                    for string in strings.into_iter().flatten() {
                        builder.append_value(
                            DateTime::<Utc>::from_utc(
                                NaiveDateTime::parse_from_str(string, TIMESTAMP_FMT)
                                    .map_err(|error| DataFusionError::External(Box::new(error)))?,
                                Utc,
                            )
                            .timestamp(),
                        )?;
                    }

                    ColumnarValue::Array(Arc::new(builder.finish()))
                }
            };

            Ok(result)
        }),
    );

    let tmp_schema = DFSchema::try_from_qualified_schema(
        "cdx",
        &Schema::new(vec![Field::new(
            "archived_at",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]),
    )?;

    let result = df.select(vec![
        col("url"),
        parse_timestamp
            .call(vec![col("archived_at")])
            .alias("archived_at")
            .cast_to(&DataType::Timestamp(TimeUnit::Second, None), &tmp_schema)?,
        col("digest"),
    ])?;
    /*let result = result.sort(vec![
        col("digest").sort(true, false),
        col("archived_at").sort(true, false),
        col("url").sort(true, false),
    ])?;*/

    println!("{:?}", result.schema());

    let parquet_options = parquet::file::properties::WriterProperties::builder()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(parquet::basic::Compression::ZSTD)
        .build();

    result.write_parquet("test", Some(parquet_options)).await?;

    Ok(())
}

/*
fn main() {
    let args = std::env::args().collect::<Vec<_>>();

    let file = File::open(&args[1]).unwrap();
    let reader = hst_cdx::open_csv_reader(file).unwrap();

    for batch in reader {
        let batch = batch.unwrap();
        println!("{:?}", batch);
    }
    //let file = File::open("test.csv").unwrap();
    //let input = read_csv(file).unwrap();

    //hst_cdx::write_file(input.into_iter(), "test.parquet", 128).unwrap();
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
*/
