#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Parsing error")]
    Parse(#[from] super::model::ParseError),
    #[error("File store error")]
    Store(#[from] super::store::Error),
    #[error("HTTP client error")]
    Reqwest(#[from] reqwest::Error),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
}
