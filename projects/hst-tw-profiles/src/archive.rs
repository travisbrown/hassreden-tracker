use crate::stream::UserInfo;
use bzip2::read::MultiBzDecoder;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tar::Archive;
use zip::ZipArchive;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Profile stream error")]
    ProfileStream(#[from] crate::stream::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("ZIP error")]
    Zip(#[from] zip::result::ZipError),
    #[error("Other error")]
    Other(String),
}

pub fn extract_tar<
    P: AsRef<Path>,
    F: FnMut(Result<Option<UserInfo>, Error>) -> Result<(), Error>,
>(
    path: P,
    mut f: F,
) -> Result<(), Error> {
    let bz2_ext = OsStr::new("bz2");

    let file = File::open(path)?;
    let mut archive = Archive::new(file);

    for entry_res in archive.entries()? {
        let entry = entry_res?;
        let path = entry.path()?;

        if path.extension() == Some(bz2_ext) {
            let reader = BufReader::new(MultiBzDecoder::new(entry));
            for line in reader.lines() {
                let result = line
                    .map_err(Error::from)
                    .and_then(|line| serde_json::from_str(&line).map_err(Error::from))
                    .and_then(|value| {
                        crate::stream::extract_user_info(&value, true).map_err(Error::from)
                    });
                f(result)?
            }
        }
    }

    Ok(())
}

pub fn extract_zip<
    P: AsRef<Path>,
    F: FnMut(Result<Option<UserInfo>, Error>) -> Result<(), Error>,
>(
    path: P,
    mut f: F,
) -> Result<(), Error> {
    let file = File::open(path)?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let file_name = file.name();
        if file_name.ends_with("bz2") {
            let reader = BufReader::new(MultiBzDecoder::new(file));
            for line in reader.lines() {
                let result = line
                    .map_err(Error::from)
                    .and_then(|line| serde_json::from_str(&line).map_err(Error::from))
                    .and_then(|value| {
                        crate::stream::extract_user_info(&value, true).map_err(Error::from)
                    });
                f(result)?
            }
        }
    }

    Ok(())
}
