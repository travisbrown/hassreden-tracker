use super::{Batch, Change};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use integer_encoding::{VarIntReader, VarIntWriter};
use std::fs::{File, ReadDir};
use std::io::{BufReader, Read};
use std::iter::Peekable;
use std::path::Path;

const HEADER_LEN: usize = 28;
const MAX_ENTRY_LEN: u32 = u32::MAX / 4;

pub fn read_dir<P: AsRef<Path>>(base: P) -> Box<dyn Iterator<Item = Result<Batch, Error>>> {
    std::fs::read_dir(base)
        .and_then(|entries| {
            let mut paths = entries
                .map(|result| result.map(|entry| entry.path()))
                .collect::<Result<Vec<_>, _>>()?;
            paths.sort();

            Ok(paths)
        })
        .map_or_else::<Box<dyn Iterator<Item = Result<Batch, Error>>>, _, _>(
            |error| Box::new(std::iter::once(Err(Error::from(error)))),
            |paths| {
                Box::new(paths.into_iter().flat_map(|path| {
                    File::open(path)
                        .map_or_else::<Box<dyn Iterator<Item = Result<Batch, Error>>>, _, _>(
                            |error| Box::new(std::iter::once(Err(Error::from(error)))),
                            |file| Box::new(FollowReader::new(BufReader::new(file))),
                        )
                }))
            },
        )
}

pub struct FollowReader<R> {
    reader: R,
    header_buffer: [u8; HEADER_LEN],
}

impl<R: Read> Iterator for FollowReader<R> {
    type Item = Result<Batch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_header().map_or_else(
            |error| Some(Err(error)),
            |header| {
                header.map(|(timestamp, user_id, follower_lens, followed_lens)| {
                    let follower_change = follower_lens.map_or_else(
                        || Ok(None),
                        |(addition_len, removal_len)| {
                            let addition_ids = read_ids(&mut self.reader, addition_len)?;
                            let removal_ids = read_ids(&mut self.reader, removal_len)?;

                            if !is_increasing(&addition_ids) {
                                Err(Error::UnsortedIds(addition_ids))
                            } else if !is_increasing(&removal_ids) {
                                Err(Error::UnsortedIds(removal_ids))
                            } else {
                                Ok(Some(Change {
                                    addition_ids,
                                    removal_ids,
                                }))
                            }
                        },
                    )?;

                    let followed_change = followed_lens.map_or_else(
                        || Ok(None),
                        |(addition_len, removal_len)| {
                            let addition_ids = read_ids(&mut self.reader, addition_len)?;
                            let removal_ids = read_ids(&mut self.reader, removal_len)?;

                            if !is_increasing(&addition_ids) {
                                Err(Error::UnsortedIds(addition_ids))
                            } else if !is_increasing(&removal_ids) {
                                Err(Error::UnsortedIds(removal_ids))
                            } else {
                                Ok(Some(Change {
                                    addition_ids,
                                    removal_ids,
                                }))
                            }
                        },
                    )?;

                    Ok(Batch {
                        timestamp,
                        user_id,
                        follower_change,
                        followed_change,
                    })
                })
            },
        )
    }
}

impl<R: Read> FollowReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            header_buffer: [0; HEADER_LEN],
        }
    }

    fn read_header(
        &mut self,
    ) -> Result<
        Option<(
            DateTime<Utc>,
            u64,
            Option<(usize, usize)>,
            Option<(usize, usize)>,
        )>,
        Error,
    > {
        match self.reader.read_exact(&mut self.header_buffer) {
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(Error::from(error)),
            Ok(()) => {
                let timestamp_s = u32::from_be_bytes(
                    self.header_buffer[0..4]
                        .try_into()
                        .map_err(|_| Error::InvalidHeader(self.header_buffer))?,
                );
                let user_id = u64::from_be_bytes(
                    self.header_buffer[4..12]
                        .try_into()
                        .map_err(|_| Error::InvalidHeader(self.header_buffer))?,
                );
                let follower_addition_len = u32::from_be_bytes(
                    self.header_buffer[12..16]
                        .try_into()
                        .map_err(|_| Error::InvalidHeader(self.header_buffer))?,
                );
                let follower_removal_len = u32::from_be_bytes(
                    self.header_buffer[16..20]
                        .try_into()
                        .map_err(|_| Error::InvalidHeader(self.header_buffer))?,
                );
                let followed_addition_len = u32::from_be_bytes(
                    self.header_buffer[20..24]
                        .try_into()
                        .map_err(|_| Error::InvalidHeader(self.header_buffer))?,
                );
                let followed_removal_len = u32::from_be_bytes(
                    self.header_buffer[24..28]
                        .try_into()
                        .map_err(|_| Error::InvalidHeader(self.header_buffer))?,
                );

                validate_header_lengths(
                    follower_addition_len,
                    follower_removal_len,
                    followed_addition_len,
                    followed_removal_len,
                )
                .map_or_else(
                    || Err(Error::InvalidHeader(self.header_buffer)),
                    |(follower_lens, followed_lens)| {
                        Ok(Some((
                            Utc.timestamp(timestamp_s.into(), 0),
                            user_id,
                            follower_lens,
                            followed_lens,
                        )))
                    },
                )
            }
        }
    }
}

fn validate_header_lengths(
    follower_addition_len: u32,
    follower_removal_len: u32,
    followed_addition_len: u32,
    followed_removal_len: u32,
) -> Option<(Option<(usize, usize)>, Option<(usize, usize)>)> {
    let follower_addition_empty = follower_addition_len == u32::MAX;
    let follower_removal_empty = follower_removal_len == u32::MAX;
    let followed_addition_empty = followed_addition_len == u32::MAX;
    let followed_removal_empty = followed_removal_len == u32::MAX;

    if (follower_addition_empty != follower_removal_empty)
        || (followed_addition_empty != followed_removal_empty)
        || (follower_addition_empty && followed_addition_empty)
    {
        None
    } else {
        Some((
            if follower_addition_empty
                || (follower_addition_len > MAX_ENTRY_LEN || follower_removal_len > MAX_ENTRY_LEN)
            {
                None
            } else {
                Some((
                    follower_addition_len as usize,
                    follower_removal_len as usize,
                ))
            },
            if followed_addition_empty
                || (followed_addition_len > MAX_ENTRY_LEN || followed_removal_len > MAX_ENTRY_LEN)
            {
                None
            } else {
                Some((
                    followed_addition_len as usize,
                    followed_removal_len as usize,
                ))
            },
        ))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Invalid header")]
    InvalidHeader([u8; HEADER_LEN]),
    #[error("Unexpected bytes")]
    UnexpectedBytes(Vec<u8>),
    #[error("Unsorted IDs")]
    UnsortedIds(Vec<u64>),
    #[error("Error reading IDs")]
    InvalidIds,
}

fn read_ids<R: Read>(reader: &mut R, len: usize) -> Result<Vec<u64>, Error> {
    let mut result = Vec::with_capacity(len);
    let mut last = 0;

    for _ in 0..len {
        last += reader.read_varint::<u64>()?;
        result.push(last);
    }

    Ok(result)
}

fn is_increasing(ids: &[u64]) -> bool {
    for i in 1..ids.len() {
        if ids[i - 1] >= ids[i] {
            return false;
        }
    }

    true
}

pub fn write_batches<
    W: std::io::Write,
    E: From<std::io::Error>,
    I: Iterator<Item = Result<Batch, E>>,
>(
    writer: &mut W,
    batches: I,
) -> Result<usize, E> {
    let mut count = 0;

    for batch in batches {
        let batch = batch?;

        writer.write_all(&(batch.timestamp.timestamp() as u32).to_be_bytes())?;
        writer.write_all(&batch.user_id.to_be_bytes())?;

        match &batch.follower_change {
            Some(change) => {
                writer.write_all(&(change.addition_ids.len() as u32).to_be_bytes())?;
                writer.write_all(&(change.removal_ids.len() as u32).to_be_bytes())?;
            }
            None => {
                writer.write_all(&u32::MAX.to_be_bytes())?;
                writer.write_all(&u32::MAX.to_be_bytes())?;
            }
        }

        match &batch.followed_change {
            Some(change) => {
                writer.write_all(&(change.addition_ids.len() as u32).to_be_bytes())?;
                writer.write_all(&(change.removal_ids.len() as u32).to_be_bytes())?;
            }
            None => {
                writer.write_all(&u32::MAX.to_be_bytes())?;
                writer.write_all(&u32::MAX.to_be_bytes())?;
            }
        }

        if let Some(change) = batch.follower_change {
            write_all(writer, &change.addition_ids)?;
            write_all(writer, &change.removal_ids)?;
        }

        if let Some(change) = batch.followed_change {
            write_all(writer, &change.addition_ids)?;
            write_all(writer, &change.removal_ids)?;
        }

        count += 1;
    }

    Ok(count)
}

/// Compress a set of integers.
///
/// The input may include duplicates and does not have to be sorted.
fn write_all<W: std::io::Write>(writer: &mut W, values: &[u64]) -> Result<(), std::io::Error> {
    if !values.is_empty() {
        writer.write_varint(values[0]);

        for delta in values.windows(2).map(|pair| pair[1] - pair[0]) {
            writer.write_varint(delta)?;
        }
    }

    Ok(())
}

pub fn date_partition_batches<E, I: Iterator<Item = Result<Batch, E>>>(
    batches: I,
) -> DateBatches<I> {
    DateBatches {
        underlying: batches.peekable(),
    }
}

pub struct DateBatches<I: Iterator> {
    underlying: Peekable<I>,
}

impl<E, I: Iterator<Item = Result<Batch, E>>> Iterator for DateBatches<I> {
    type Item = Result<(NaiveDate, Vec<Batch>), E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|result| {
            let batch = result?;
            let date = batch.timestamp.date_naive();
            let mut batches = vec![batch];

            while let Some(next) = self.underlying.next_if(|result| {
                result
                    .as_ref()
                    .map_or(false, |batch| batch.timestamp.date_naive() == date)
            }) {
                // We've just checked for failure so this will always add an element.
                if let Ok(batch) = next {
                    batches.push(batch);
                }
            }

            Ok((date, batches))
        })
    }
}
