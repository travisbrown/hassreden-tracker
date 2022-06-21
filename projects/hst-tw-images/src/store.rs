use super::{model::ParseError, Image, ImageKey};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

const DOMAIN_DIR_SIZE: usize = 1;
const PREFIX_DIR_SIZE: usize = 100;
const FILE_DIR_SIZE: usize = 1000;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Parsing error")]
    Parse(#[from] ParseError),
    #[error("Invalid directory")]
    InvalidDirectory(Box<Path>),
    #[error("Invalid file")]
    InvalidFile(Box<Path>),
    #[error("Encoding error")]
    Encoding(#[from] bincode::error::EncodeError),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
}

pub(crate) fn is_valid_prefix_dir<P: AsRef<Path>>(path: P) -> bool {
    lazy_static::lazy_static! {
        static ref PREFIX_DIR_RE: regex::Regex = regex::Regex::new(r"^\d\d$").unwrap();
    }

    path.as_ref()
        .file_name()
        .and_then(|value| value.to_str())
        .map(|value| PREFIX_DIR_RE.is_match(value))
        .unwrap_or(false)
}

pub struct Store {
    base: PathBuf,
}

impl Store {
    pub fn new<P: AsRef<Path>>(base: P) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
        }
    }

    pub fn keys(&self) -> StoreIterator<ImageKey, ImageKeyExtractor> {
        StoreIterator::new(self.base.as_path())
    }

    pub fn write_keys<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        let config = bincode::config::standard();

        for entry in self.keys() {
            let key = entry?;

            bincode::encode_into_std_write(key, writer, config)?;
        }

        Ok(())
    }

    pub fn path<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.base.join(path)
    }
}

impl IntoIterator for &Store {
    type Item = Result<(Image, PathBuf), Error>;
    type IntoIter = StoreIterator<(Image, PathBuf), ImagePathExtractor>;
    fn into_iter(self) -> Self::IntoIter {
        StoreIterator::new(self.base.as_path())
    }
}

pub struct StoreIterator<T, F> {
    base: Option<PathBuf>,
    domain_dirs: Vec<PathBuf>,
    prefix_a_dirs: Vec<PathBuf>,
    prefix_b_dirs: Vec<PathBuf>,
    files: Vec<T>,
    _f: PhantomData<F>,
}

impl<T, F> StoreIterator<T, F> {
    fn new(base: &Path) -> Self {
        Self {
            base: Some(base.to_path_buf()),
            domain_dirs: Vec::with_capacity(DOMAIN_DIR_SIZE),
            prefix_a_dirs: Vec::with_capacity(PREFIX_DIR_SIZE),
            prefix_b_dirs: Vec::with_capacity(PREFIX_DIR_SIZE),
            files: Vec::with_capacity(FILE_DIR_SIZE),
            _f: PhantomData,
        }
    }
}

impl<T, F: FileExtractor<Output = T>> Iterator for StoreIterator<T, F> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next_file_value) = self.files.pop() {
            Some(Ok(next_file_value))
        } else if let Some(next_prefix_b_dir) = self.prefix_b_dirs.pop() {
            if is_valid_prefix_dir(&next_prefix_b_dir) {
                read_paths_with::<F>(&next_prefix_b_dir, &mut self.files)
                    .map_or_else(|error| Some(Err(error)), |_| self.next())
            } else {
                Some(Err(Error::InvalidDirectory(next_prefix_b_dir.into())))
            }
        } else if let Some(next_prefix_a_dir) = self.prefix_a_dirs.pop() {
            if is_valid_prefix_dir(&next_prefix_a_dir) {
                read_paths(&next_prefix_a_dir, &mut self.prefix_b_dirs)
                    .map_or_else(|error| Some(Err(error)), |_| self.next())
            } else {
                Some(Err(Error::InvalidDirectory(next_prefix_a_dir.into())))
            }
        } else if let Some(next_domain_dir) = self.domain_dirs.pop() {
            read_paths(&next_domain_dir, &mut self.prefix_a_dirs)
                .map_or_else(|error| Some(Err(error)), |_| self.next())
        } else if let Some(base_dir) = self.base.take() {
            read_paths(&base_dir, &mut self.domain_dirs)
                .map_or_else(|error| Some(Err(error)), |_| self.next())
        } else {
            None
        }
    }
}

pub trait FileExtractor {
    type Output;

    fn apply(path: PathBuf) -> Result<Self::Output, Error>;
}

pub struct ImagePathExtractor;

impl FileExtractor for ImagePathExtractor {
    type Output = (Image, PathBuf);

    fn apply(path: PathBuf) -> Result<Self::Output, Error> {
        Image::try_from(path.as_path())
            .map(|image| (image, path))
            .map_err(Error::from)
    }
}

pub struct ImageKeyExtractor;

impl FileExtractor for ImageKeyExtractor {
    type Output = ImageKey;

    fn apply(path: PathBuf) -> Result<Self::Output, Error> {
        Image::try_from(path.as_path())
            .map(|image| image.key())
            .map_err(Error::from)
    }
}

fn read_paths(dir: &Path, result: &mut Vec<PathBuf>) -> Result<(), Error> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;

        result.push(entry.path());
    }

    result.sort();

    Ok(())
}

fn read_paths_with<F: FileExtractor>(dir: &Path, result: &mut Vec<F::Output>) -> Result<(), Error> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;

        result.push(F::apply(entry.path())?);
    }

    Ok(())
}
