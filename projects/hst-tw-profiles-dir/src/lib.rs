use chrono::NaiveDate;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use zstd::stream::read::Decoder as ZstDecoder;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("Invalid path")]
    Path(Box<Path>),
}

pub struct ProfilesDir {
    paths: Vec<(NaiveDate, PathBuf)>,
}

impl ProfilesDir {
    pub fn open<P: AsRef<Path>>(base: P) -> Result<ProfilesDir, Error> {
        let mut paths = vec![];

        for entry in std::fs::read_dir(base)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();

            let date = if file_type.is_file() {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|file_name| {
                        if file_name.len() == 21 && file_name.ends_with(".ndjson.zst") {
                            NaiveDate::parse_from_str(&file_name[..10], "%Y-%m-%d").ok()
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| Error::Path(path.clone().into_boxed_path()))
            } else {
                Err(Error::Path(path.clone().into_boxed_path()))
            }?;

            paths.push((date, path));
        }

        paths.sort_unstable();

        Ok(ProfilesDir { paths })
    }

    pub fn profiles(
        &self,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> impl Iterator<Item = Result<Value, Error>> + '_ {
        let start = start.unwrap_or(NaiveDate::MIN);
        let end = end.unwrap_or(NaiveDate::MAX);

        self.paths
            .iter()
            .skip_while(move |(date, _)| *date < start)
            .take_while(move |(date, _)| *date <= end)
            .flat_map(|(_, path)| Self::read_ndjson(path))
    }

    fn read_ndjson<P: AsRef<Path>>(path: P) -> Box<dyn Iterator<Item = Result<Value, Error>>> {
        match File::open(path)
            .and_then(ZstDecoder::new)
            .map_err(Error::from)
        {
            Ok(decoder) => Box::new(BufReader::new(decoder).lines().map(|line| {
                let line = line?;
                Ok(serde_json::from_str(&line)?)
            })),
            Err(error) => Box::new(std::iter::once(Err(error))),
        }
    }
}
