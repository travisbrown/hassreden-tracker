use bincode::{Decode, Encode};
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::path::Path;
use std::str::FromStr;

const DEFAULT_PATH: &str = "profile_images/";

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("Invalid size")]
    InvalidSize(String),
    #[error("Invalid URL")]
    InvalidUrl(String),
    #[error("Invalid file name")]
    InvalidFileName(String),
    #[error("Invalid profile image ID")]
    InvalidId(String),
    #[error("Invalid path")]
    InvalidPath(Box<Path>),
}

#[derive(Copy, Clone, Debug, Decode, Encode, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Size {
    Mini,
    Normal,
    Bigger,
    Square200,
    Square400,
}

impl std::fmt::Display for Size {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Mini => "mini",
            Self::Normal => "normal",
            Self::Bigger => "bigger",
            Self::Square200 => "200x200",
            Self::Square400 => "400x400",
        };
        write!(f, "{}", value)
    }
}

impl FromStr for Size {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mini" => Ok(Self::Mini),
            "normal" => Ok(Self::Normal),
            "bigger" => Ok(Self::Bigger),
            "200x200" => Ok(Self::Square200),
            "400x400" => Ok(Self::Square400),
            _ => Err(Self::Err::InvalidSize(s.to_string())),
        }
    }
}

#[derive(Clone, Debug, Decode, Encode, Eq, Hash, PartialEq)]
pub enum Domain {
    Pbs,
    Si0,
    Other(String),
}

impl std::fmt::Display for Domain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Pbs => "pbs.twimg.com",
            Self::Si0 => "si0.twimg.com",
            Self::Other(value) => value,
        };
        write!(f, "{}", value)
    }
}

impl FromStr for Domain {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pbs.twimg.com" => Ok(Self::Pbs),
            "si0.twimg.com" => Ok(Self::Si0),
            value => Ok(Self::Other(value.to_string())),
        }
    }
}

impl Default for Domain {
    fn default() -> Self {
        Self::Pbs
    }
}

#[derive(Clone, Debug, Decode, Encode, Eq, Hash, PartialEq)]
pub struct ImageKey {
    domain: Domain,
    id: u64,
    name: String,
    extension: Option<String>,
}

impl ImageKey {
    pub fn to_image(&self, size: Size) -> Image {
        Image {
            domain: self.domain.clone(),
            id: self.id,
            name: self.name.clone(),
            size,
            extension: self.extension.clone(),
        }
    }
}

#[derive(Clone, Debug, Decode, Encode, Eq, Hash, PartialEq)]
pub struct Image {
    pub domain: Domain,
    pub id: u64,
    pub name: String,
    pub size: Size,
    pub extension: Option<String>,
}

impl Image {
    pub fn key(&self) -> ImageKey {
        ImageKey {
            domain: self.domain.clone(),
            id: self.id,
            name: self.name.clone(),
            extension: self.extension.clone(),
        }
    }

    pub fn with_size(&self, size: Size) -> Self {
        Self {
            domain: self.domain.clone(),
            id: self.id,
            name: self.name.clone(),
            size,
            extension: self.extension.clone(),
        }
    }

    pub fn extension_string(&self) -> String {
        self.extension
            .as_ref()
            .map(|value| format!(".{}", value))
            .unwrap_or_default()
    }

    pub fn url(&self) -> String {
        format!("{}", self)
    }

    pub fn id_prefix_url(&self) -> String {
        format!("https://{}/{}{}/", self.domain, DEFAULT_PATH, self.id)
    }

    /// Convert the ID to a string and split off the last four characters.
    ///
    /// This approach allows us to avoid directories containing millions of files, which can
    /// cause problems in some contexts. We use the ID because it's known to be numeric, so we
    /// don't have to worry about escaping characters, and we use the final digits for balance.
    fn path_dir_prefix(&self) -> (String, String) {
        let mut chars = self
            .id
            .to_string()
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>();

        // If the ID as a string has fewer than four characters, we pad it with zeroes.
        while chars.len() < 4 {
            chars.push('0');
        }

        chars.reverse();

        (chars[0..2].iter().collect(), chars[2..4].iter().collect())
    }

    pub fn path(&self) -> String {
        let (prefix_a, prefix_b) = self.path_dir_prefix();

        format!(
            "{}/{}/{}/{}-{}_{}{}",
            self.domain,
            prefix_a,
            prefix_b,
            self.id,
            self.name,
            self.size,
            self.extension_string()
        )
    }

    fn parse_url_file_name(input: &str) -> Result<(String, Size, Option<String>), ParseError> {
        lazy_static::lazy_static! {
            static ref URL_FILE_NAME_RE: regex::Regex = regex::Regex::new(
                r"^(.*)_([^\.]+)(\.[a-zA-Z0-9-]+)?$"
            )
            .unwrap();
        }

        let ((name_match, size_match), extension_match) = URL_FILE_NAME_RE
            .captures(input)
            .and_then(|captures| {
                captures
                    .get(1)
                    .zip(captures.get(2))
                    .map(|value| (value, captures.get(3)))
            })
            .ok_or_else(|| ParseError::InvalidFileName(input.to_string()))?;

        let name_source = name_match.as_str();
        let size_source = size_match.as_str();
        let extension = extension_match.map(|value| value.as_str()[1..].to_string());

        let name = name_source.to_string();
        let size = size_source.parse()?;

        Ok((name, size, extension))
    }

    pub fn parse_file_name(input: &str) -> Option<Image> {
        lazy_static::lazy_static! {
            static ref FILE_NAME_RE: regex::Regex = regex::Regex::new(
                r"^(\d+)\-(.*)$"
            )
            .unwrap();
        }

        let (id_match, rest_match) = FILE_NAME_RE
            .captures(input)
            .and_then(|captures| captures.get(1).zip(captures.get(2)))?;

        let id = id_match.as_str().parse::<u64>().ok()?;
        let (name, size, extension) = Self::parse_url_file_name(rest_match.as_str()).ok()?;

        Some(Self {
            domain: Domain::default(),
            id,
            name,
            size,
            extension,
        })
    }
}

impl std::fmt::Display for Image {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "https://{}/{}{}/{}_{}{}",
            self.domain,
            DEFAULT_PATH,
            self.id,
            self.name,
            self.size,
            self.extension_string()
        )
    }
}

impl FromStr for Image {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static::lazy_static! {
            static ref URL_RE: regex::Regex = regex::Regex::new(
                r"^https?://([^/]+)/profile_images/(\d+)/(.*)$"
            )
            .unwrap();
        }

        let ((domain_match, id_match), rest_match) = URL_RE
            .captures(s)
            .and_then(|captures| captures.get(1).zip(captures.get(2)).zip(captures.get(3)))
            .ok_or_else(|| Self::Err::InvalidUrl(s.to_string()))?;

        let domain_source = domain_match.as_str();
        let domain = domain_source.parse()?;

        let id = id_match
            .as_str()
            .parse::<u64>()
            .map_err(|_| ParseError::InvalidId(id_match.as_str().to_string()))?;
        let (name, size, extension) = Self::parse_url_file_name(rest_match.as_str())?;

        Ok(Self {
            domain,
            id,
            name,
            size,
            extension,
        })
    }
}

impl TryFrom<&Path> for Image {
    type Error = ParseError;

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        let mut image = path_to_str(value)
            .and_then(Self::parse_file_name)
            .ok_or_else(|| Self::Error::InvalidPath(value.into()))?;

        let (domain, (prefix_a, prefix_b)) = get_parents_3(value)
            .filter(|(_, prefix_a_dir, prefix_b_dir)| {
                super::store::is_valid_prefix_dir(prefix_a_dir)
                    && super::store::is_valid_prefix_dir(prefix_b_dir)
            })
            .and_then(|(domain_dir, prefix_a_dir, prefix_b_dir)| {
                path_to_str(domain_dir)
                    .zip(path_to_str(prefix_a_dir).zip(path_to_str(prefix_b_dir)))
            })
            .ok_or_else(|| Self::Error::InvalidPath(value.into()))?;

        let id_str = image.id.to_string();
        let id_len = id_str.len();

        if id_len >= 4
            && prefix_a == &id_str[id_len - 4..id_len - 2]
            && prefix_b == &id_str[id_len - 2..]
        {
            image.domain = domain.parse()?;

            Ok(image)
        } else {
            Err(Self::Error::InvalidPath(value.into()))
        }
    }
}

fn path_to_str(path: &Path) -> Option<&str> {
    path.file_name().and_then(|value| value.to_str())
}

fn get_parents_3(path: &Path) -> Option<(&Path, &Path, &Path)> {
    let parent_3 = path.parent()?;
    let parent_2 = parent_3.parent()?;
    let parent_1 = parent_2.parent()?;

    Some((parent_1, parent_2, parent_3))
}
