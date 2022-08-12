use super::{Batch, Change};
use chrono::{DateTime, TimeZone, Utc};
use std::io::{Cursor, Read};

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
                        let count = self.reader.read(&mut batch_buffer)?;

                        if count == batch_len {
                            let mut cursor = Cursor::new(batch_buffer);

                            let follower_addition_ids =
                                read_ids(&mut cursor, follower_addition_len)?;
                            let mut follower_removal_ids =
                                read_ids(&mut cursor, follower_removal_len)?;
                            let mut followed_addition_ids =
                                read_ids(&mut cursor, followed_addition_len)?;
                            let mut followed_removal_ids =
                                read_ids(&mut cursor, followed_removal_len)?;

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
                        } else {
                            Err(Error::UnexpectedBytes(batch_buffer[0..count].to_vec()))
                        }
                    },
                )
            },
        )
    }
}

impl<R: Read> FollowReader<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            header_buffer: [0; HEADER_LEN],
        }
    }

    fn read_header(
        &mut self,
    ) -> Result<Option<(DateTime<Utc>, u64, usize, usize, usize, usize)>, Error> {
        let count = self.reader.read(&mut self.header_buffer)?;

        if count == 0 {
            Ok(None)
        } else if count == HEADER_LEN {
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
        } else {
            Err(Error::UnexpectedBytes(
                self.header_buffer[0..count].to_vec(),
            ))
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

        for id in batch.follower_change.addition_ids {
            writer.write_all(&id.to_be_bytes());
        }

        for id in batch.follower_change.removal_ids {
            writer.write_all(&id.to_be_bytes());
        }

        for id in batch.followed_change.addition_ids {
            writer.write_all(&id.to_be_bytes());
        }

        for id in batch.followed_change.removal_ids {
            writer.write_all(&id.to_be_bytes());
        }

        count += 1;
    }

    Ok(count)
}
