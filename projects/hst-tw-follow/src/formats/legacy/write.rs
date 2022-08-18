use super::super::{Batch, Change};
use flate2::{write::GzEncoder, Compression};
use integer_encoding::VarIntWriter;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub fn write_batches<
    P: AsRef<Path>,
    E: From<std::io::Error>,
    I: Iterator<Item = Result<Batch, E>>,
>(
    base: P,
    batches: I,
) -> Result<(), E> {
    let mut followers_seen = HashSet::new();
    let mut following_seen = HashSet::new();

    for result in batches {
        let batch = result?;
        let timestamp = batch.timestamp.timestamp();
        let user_dir = base.as_ref().join(format!("{:0>20}", batch.user_id));
        let followers_dir = user_dir.join(super::FOLLOWERS_DIR_NAME);
        let following_dir = user_dir.join(super::FOLLOWING_DIR_NAME);

        std::fs::create_dir_all(&followers_dir)?;
        std::fs::create_dir_all(&following_dir)?;

        if followers_seen.contains(&batch.user_id) {
            if let Some(change) = batch.follower_change {
                write_txt(&followers_dir, timestamp, change)?;
            }
        } else if let Some(change) = batch.follower_change {
            write_gz(&followers_dir, timestamp, change)?;
            followers_seen.insert(batch.user_id);
        }

        if following_seen.contains(&batch.user_id) {
            if let Some(change) = batch.followed_change {
                write_txt(&following_dir, timestamp, change)?;
            }
        } else if let Some(change) = batch.followed_change {
            write_gz(&following_dir, timestamp, change)?;
            following_seen.insert(batch.user_id);
        }
    }

    Ok(())
}

fn write_txt<P: AsRef<Path>>(dir: P, timestamp: i64, change: Change) -> Result<(), std::io::Error> {
    let path = dir.as_ref().join(format!("{}.txt", timestamp));
    let mut writer = BufWriter::new(File::create(path)?);

    for id in change.addition_ids {
        writeln!(writer, "{}", id)?;
    }

    for id in change.removal_ids {
        writeln!(writer, "-{}", id)?;
    }

    Ok(())
}

fn write_gz<P: AsRef<Path>>(dir: P, timestamp: i64, change: Change) -> Result<(), std::io::Error> {
    let path = dir.as_ref().join(format!("{}.gz", timestamp));
    let mut writer = GzEncoder::new(File::create(path)?, Compression::default());

    writer.write_varint(change.addition_ids.len())?;

    if !change.addition_ids.is_empty() {
        writer.write_varint(change.addition_ids[0])?;

        for delta in change.addition_ids.windows(2).map(|pair| pair[1] - pair[0]) {
            writer.write_varint(delta)?;
        }
    }

    writer.try_finish()?;

    Ok(())
}
