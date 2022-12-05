//! Library for working with user profiles from the Twitter API.

use apache_avro::Reader as AvroReader;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader, Lines, Read};
use std::path::Path;
use zstd::stream::read::Decoder as ZstDecoder;

pub mod archive;
pub mod avro;
pub mod model;
pub mod stream;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Avro error")]
    Avro(#[from] apache_avro::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("Invalid path")]
    Path(Box<Path>),
}

pub enum ProfileReader<'a, R: Read> {
    NdJson(Lines<BufReader<R>>),
    NdJsonGz(Lines<BufReader<GzDecoder<R>>>),
    NdJsonZst(Lines<BufReader<ZstDecoder<'a, BufReader<R>>>>),
    Avro(AvroReader<'a, R>),
    Failed(Option<Error>),
}

impl ProfileReader<'static, File> {
    pub fn open<P: AsRef<Path>>(path: P) -> ProfileReader<'static, File> {
        if let Some(file_name) = path
            .as_ref()
            .file_name()
            .and_then(|file_name| file_name.to_str())
        {
            if file_name.ends_with(".avro") {
                match File::open(path)
                    .map_err(Error::from)
                    .and_then(|file| Ok(avro::reader(file)?))
                {
                    Ok(reader) => ProfileReader::Avro(reader),
                    Err(error) => ProfileReader::Failed(Some(error)),
                }
            } else if file_name.ends_with(".ndjson") {
                match File::open(path).map_err(Error::from) {
                    Ok(file) => ProfileReader::NdJson(BufReader::new(file).lines()),
                    Err(error) => ProfileReader::Failed(Some(error)),
                }
            } else if file_name.ends_with(".ndjson.gz") {
                match File::open(path).map_err(Error::from) {
                    Ok(file) => {
                        ProfileReader::NdJsonGz(BufReader::new(GzDecoder::new(file)).lines())
                    }
                    Err(error) => ProfileReader::Failed(Some(error)),
                }
            } else if file_name.ends_with(".ndjson.zst") {
                match File::open(path)
                    .map_err(Error::from)
                    .and_then(|file| Ok(ZstDecoder::new(file)?))
                {
                    Ok(decoder) => ProfileReader::NdJsonZst(BufReader::new(decoder).lines()),
                    Err(error) => ProfileReader::Failed(Some(error)),
                }
            } else {
                ProfileReader::Failed(Some(Error::Path(
                    path.as_ref().to_path_buf().into_boxed_path(),
                )))
            }
        } else {
            ProfileReader::Failed(Some(Error::Path(
                path.as_ref().to_path_buf().into_boxed_path(),
            )))
        }
    }
}

impl<'a, R: Read> ProfileReader<'a, R> {
    fn next_user<B: BufRead>(lines: &mut Lines<B>) -> Option<Result<model::User, Error>> {
        lines.next().map(|line| Ok(serde_json::from_str(&line?)?))
    }

    pub fn into_json_iter(self) -> ProfileJsonReader<'a, R> {
        ProfileJsonReader(self)
    }
}

impl<'a, R: Read> Iterator for ProfileReader<'a, R> {
    type Item = Result<model::User, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ProfileReader::NdJson(lines) => Self::next_user(lines),
            ProfileReader::NdJsonGz(lines) => Self::next_user(lines),
            ProfileReader::NdJsonZst(lines) => Self::next_user(lines),
            ProfileReader::Avro(reader) => reader
                .next()
                .map(|value| Ok(apache_avro::from_value::<model::User>(&value?)?)),
            ProfileReader::Failed(error) => error.take().map(Err),
        }
    }
}

pub struct ProfileJsonReader<'a, R: Read>(ProfileReader<'a, R>);

impl<'a, R: Read> ProfileJsonReader<'a, R> {
    fn next_json<B: BufRead>(lines: &mut Lines<B>) -> Option<Result<serde_json::Value, Error>> {
        lines.next().map(|line| Ok(serde_json::from_str(&line?)?))
    }
}

impl<'a, R: Read> Iterator for ProfileJsonReader<'a, R> {
    type Item = Result<serde_json::Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            ProfileReader::NdJson(lines) => Self::next_json(lines),
            ProfileReader::NdJsonGz(lines) => Self::next_json(lines),
            ProfileReader::NdJsonZst(lines) => Self::next_json(lines),
            ProfileReader::Avro(reader) => reader
                .next()
                .map(|value| Ok(apache_avro::from_value::<serde_json::Value>(&value?)?)),
            ProfileReader::Failed(error) => error.take().map(Err),
        }
    }
}
