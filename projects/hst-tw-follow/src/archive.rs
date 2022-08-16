use super::{Batch, Change};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use integer_encoding::VarIntWriter;
use std::io::{Cursor, Read};
use std::iter::Peekable;

const HEADER_LEN: usize = 28;
const MAX_ENTRY_LEN: u32 = u32::MAX / 4;

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
                header.map(
                    |(
                        timestamp,
                        user_id,
                        follower_addition_len,
                        follower_removal_len,
                        followed_addition_len,
                        followed_removal_len,
                    )| {
                        let batch_len = (follower_addition_len
                            + follower_removal_len
                            + followed_addition_len
                            + followed_removal_len)
                            * 8;

                        let mut batch_buffer = vec![0; batch_len];
                        self.reader.read_exact(&mut batch_buffer)?;

                        let mut cursor = Cursor::new(batch_buffer);

                        let follower_addition_ids = read_ids(&mut cursor, follower_addition_len)?;
                        let mut follower_removal_ids = read_ids(&mut cursor, follower_removal_len)?;
                        let mut followed_addition_ids =
                            read_ids(&mut cursor, followed_addition_len)?;
                        let mut followed_removal_ids = read_ids(&mut cursor, followed_removal_len)?;

                        if !is_increasing(&follower_addition_ids) {
                            Err(Error::UnsortedIds(follower_addition_ids))
                        } else if !is_increasing(&follower_removal_ids) {
                            Err(Error::UnsortedIds(follower_removal_ids))
                        } else if !is_increasing(&followed_addition_ids) {
                            Err(Error::UnsortedIds(followed_addition_ids))
                        } else if !is_increasing(&followed_removal_ids) {
                            Err(Error::UnsortedIds(followed_removal_ids))
                        } else {
                            Ok(Batch {
                                timestamp,
                                user_id,
                                follower_change: Change {
                                    addition_ids: follower_addition_ids,
                                    removal_ids: follower_removal_ids,
                                },
                                followed_change: Change {
                                    addition_ids: followed_addition_ids,
                                    removal_ids: followed_removal_ids,
                                },
                            })
                        }
                    },
                )
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
    ) -> Result<Option<(DateTime<Utc>, u64, usize, usize, usize, usize)>, Error> {
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

                if follower_addition_len > MAX_ENTRY_LEN
                    || follower_removal_len > MAX_ENTRY_LEN
                    || followed_addition_len > MAX_ENTRY_LEN
                    || followed_removal_len > MAX_ENTRY_LEN
                {
                    Err(Error::InvalidHeader(self.header_buffer))
                } else {
                    Ok(Some((
                        Utc.timestamp(timestamp_s.into(), 0),
                        user_id,
                        follower_addition_len as usize,
                        follower_removal_len as usize,
                        followed_addition_len as usize,
                        followed_removal_len as usize,
                    )))
                }
            }
        }
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

fn read_ids(cursor: &mut Cursor<Vec<u8>>, len: usize) -> Result<Vec<u64>, Error> {
    let mut result = Vec::with_capacity(len);
    let mut buffer = [0; 8];

    for i in 0..len {
        if cursor.read(&mut buffer)? == 8 {
            result.push(u64::from_be_bytes(buffer));
        } else {
            return Err(Error::InvalidIds);
        }
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
        writer.write_all(&(batch.follower_change.addition_ids.len() as u32).to_be_bytes())?;
        writer.write_all(&(batch.follower_change.removal_ids.len() as u32).to_be_bytes())?;
        writer.write_all(&(batch.followed_change.addition_ids.len() as u32).to_be_bytes())?;
        writer.write_all(&(batch.followed_change.removal_ids.len() as u32).to_be_bytes())?;

        write_all(writer, batch.follower_change.addition_ids)?;
        write_all(writer, batch.follower_change.removal_ids)?;
        write_all(writer, batch.followed_change.addition_ids)?;
        write_all(writer, batch.followed_change.removal_ids)?;

        /*for id in batch.follower_change.addition_ids {
            writer.write_all(&id.to_be_bytes())?;
        }

        for id in batch.follower_change.removal_ids {
            writer.write_all(&id.to_be_bytes())?;
        }

        for id in batch.followed_change.addition_ids {
            writer.write_all(&id.to_be_bytes())?;
        }

        for id in batch.followed_change.removal_ids {
            writer.write_all(&id.to_be_bytes())?;
        }*/

        count += 1;
    }

    Ok(count)
}

/// Compress a set of integers.
///
/// The input may include duplicates and does not have to be sorted.
fn write_all<W: std::io::Write, I: IntoIterator<Item = u64>>(
    writer: &mut W,
    values: I,
) -> Result<(), std::io::Error> {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.push(0);
    values.sort_unstable();
    values.dedup();

    for delta in values.windows(2).map(|pair| pair[1] - pair[0]) {
        writer.write_varint(delta)?;
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
