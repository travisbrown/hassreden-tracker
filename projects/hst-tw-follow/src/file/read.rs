use super::super::{Batch, Change};
use super::Error;
use chrono::{DateTime, TimeZone, Utc};
use flate2::read::GzDecoder;
use integer_encoding::VarIntReader;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

pub enum BatchIterator {
    Failed(Option<Error>),
    Remaining(Vec<(DateTime<Utc>, u64, Option<Box<Path>>, Option<Box<Path>>)>),
}

impl BatchIterator {
    pub fn new<P: AsRef<Path>>(base: P) -> Self {
        list_batch_files(base).map_or_else(
            |error| Self::Failed(Some(error)),
            |mut files| {
                files.reverse();
                Self::Remaining(files)
            },
        )
    }
}

impl Iterator for BatchIterator {
    type Item = Result<Batch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Failed(error) => error.take().map(Err),
            Self::Remaining(ref mut files) => {
                files
                    .pop()
                    .map(|(timestamp, user_id, followers_path, following_path)| {
                        let follower_change = followers_path
                            .map_or_else(|| Ok(None), |path| read_batch_file(path).map(Some))?;
                        let followed_change = following_path
                            .map_or_else(|| Ok(None), |path| read_batch_file(path).map(Some))?;

                        Ok(Batch {
                            timestamp,
                            user_id,
                            follower_change,
                            followed_change,
                        })
                    })
            }
        }
    }
}

fn list_batch_files<P: AsRef<Path>>(
    base: P,
) -> Result<Vec<(DateTime<Utc>, u64, Option<Box<Path>>, Option<Box<Path>>)>, Error> {
    let mut results = vec![];

    for entry in std::fs::read_dir(&base)? {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .ok_or_else(|| Error::InvalidUserDirectory(path.clone().into_boxed_path()))?;
        let user_id = name
            .chars()
            .skip_while(|ch| *ch == '0')
            .collect::<String>()
            .parse::<u64>()
            .map_err(|_| Error::InvalidUserDirectory(path.clone().into_boxed_path()))?;

        let mut followers = list_timestamped_files(path.join(super::FOLLOWERS_DIR_NAME))?;
        let mut following = list_timestamped_files(path.join(super::FOLLOWING_DIR_NAME))?;

        let timestamps = followers
            .keys()
            .chain(following.keys())
            .copied()
            .collect::<HashSet<_>>();

        for timestamp in timestamps {
            results.push((
                timestamp,
                user_id,
                followers.remove(&timestamp),
                following.remove(&timestamp),
            ));
        }
    }

    results.sort();

    Ok(results)
}

fn list_timestamped_files<P: AsRef<Path>>(
    path: P,
) -> Result<HashMap<DateTime<Utc>, Box<Path>>, Error> {
    let results = std::fs::read_dir(path)?
        .map(|batch_entry| {
            let batch_entry = batch_entry?;
            let batch_path = batch_entry.path();
            let timestamp_s = batch_path
                .file_stem()
                .and_then(|file_stem| file_stem.to_str())
                .ok_or_else(|| Error::InvalidBatchFile(batch_path.clone().into_boxed_path()))?;
            let timestamp = parse_timestamp_s(timestamp_s)?;

            Ok::<_, Error>((timestamp, batch_path.into_boxed_path()))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;

    let mut sorted = results.iter().collect::<Vec<_>>();
    sorted.sort();

    for (i, (_, path)) in sorted.into_iter().enumerate() {
        let extension = path
            .extension()
            .and_then(|extension| extension.to_str())
            .ok_or_else(|| Error::InvalidBatchFile(path.clone()))?;

        if i == 0 && extension != "gz" || i > 0 && extension != "txt" {
            return Err(Error::InvalidBatchFile(path.clone()));
        }
    }

    Ok(results)
}

fn read_batch_file<P: AsRef<Path>>(path: P) -> Result<Change, Error> {
    let extension = path
        .as_ref()
        .extension()
        .and_then(|extension| extension.to_str())
        .ok_or_else(|| Error::InvalidBatchFile(path.as_ref().to_path_buf().into_boxed_path()))?;

    let mut file = File::open(&path)?;

    if extension == "gz" {
        let addition_ids = read_gz(&mut file)?;
        Ok(Change {
            addition_ids,
            removal_ids: vec![],
        })
    } else if extension == "txt" {
        let mut addition_ids = vec![];
        let mut removal_ids = vec![];

        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            let is_removal = line.starts_with('-');

            let user_id = if is_removal {
                line[1..].parse::<u64>()
            } else {
                line.parse::<u64>()
            }
            .map_err(|_| Error::InvalidUpdateLine(line.clone()))?;

            if is_removal {
                removal_ids.push(user_id);
            } else {
                addition_ids.push(user_id);
            }
        }

        Ok(Change {
            addition_ids,
            removal_ids,
        })
    } else {
        eprintln!("{}", extension);
        Err(Error::InvalidBatchFile(
            path.as_ref().to_path_buf().into_boxed_path(),
        ))
    }
}

/// Decompress a set of integers.
///
/// The result will be sorted.
fn read_gz<R: Read>(reader: &mut R) -> Result<Vec<u64>, Error> {
    let mut reader = GzDecoder::new(reader);
    let len = reader.read_varint::<usize>()?;

    let mut values = Vec::with_capacity(len);
    let mut last = 0;

    for _ in 0..len {
        last += reader.read_varint::<u64>()?;
        values.push(last);
    }

    Ok(values)
}

fn parse_timestamp_s(input: &str) -> Result<DateTime<Utc>, Error> {
    Ok(Utc.timestamp(
        input
            .parse::<i64>()
            .map_err(|_| Error::InvalidTimestamp(input.to_string()))?,
        0,
    ))
}
