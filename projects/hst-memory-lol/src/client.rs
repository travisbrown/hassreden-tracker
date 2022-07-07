use super::Observation;
use chrono::NaiveDate;
use indexmap::IndexMap;
use reqwest::Url;
use serde::Deserialize;
use std::collections::HashMap;

const DATE_FORMAT: &str = "%Y-%m-%d";
const MEMORY_LOL_BASE: &str = "https://memory.lol/";

lazy_static::lazy_static! {
    pub static ref MEMORY_LOL_BASE_URL: Url = Url::parse(MEMORY_LOL_BASE).unwrap();
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP client error")]
    HttpClient(#[from] reqwest::Error),
    #[error("Invalid URL error")]
    Url(#[from] url::ParseError),
    #[error("Invalid date range")]
    InvalidDateRange(Vec<String>),
}

#[derive(Deserialize)]
struct ScreenNameResult {
    accounts: Vec<Account>,
}

#[derive(Deserialize)]
struct Account {
    id: u64,
    #[serde(rename = "screen-names")]
    screen_names: IndexMap<String, Option<Vec<String>>>,
}

pub struct Client {
    base: Url,
}

impl Client {
    pub fn new(base: &Url) -> Self {
        Self { base: base.clone() }
    }

    pub async fn lookup_tw_screen_name(
        &self,
        screen_name: &str,
    ) -> Result<HashMap<u64, Vec<Observation>>, Error> {
        let url = self.base.join(&format!("tw/{}", screen_name))?;
        let accounts = reqwest::get(url).await?.json::<ScreenNameResult>().await?;

        accounts
            .accounts
            .into_iter()
            .map(|account| {
                let observations = account
                    .screen_names
                    .into_iter()
                    .map(|(screen_name, range_strings)| {
                        let range = match range_strings {
                            Some(strings) => match strings.len() {
                                1 => {
                                    let value = NaiveDate::parse_from_str(&strings[0], DATE_FORMAT)
                                        .map_err(|_| Error::InvalidDateRange(strings.clone()))?;

                                    Some((value, value))
                                }
                                2 => {
                                    let first = NaiveDate::parse_from_str(&strings[0], DATE_FORMAT)
                                        .map_err(|_| Error::InvalidDateRange(strings.clone()))?;
                                    let last = NaiveDate::parse_from_str(&strings[1], DATE_FORMAT)
                                        .map_err(|_| Error::InvalidDateRange(strings.clone()))?;

                                    Some((first, last))
                                }
                                _ => Err(Error::InvalidDateRange(strings))?,
                            },
                            None => None,
                        };

                        let result: Result<Observation, Error> =
                            Ok(Observation { screen_name, range });
                        result
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok((account.id, observations))
            })
            .collect()
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new(&MEMORY_LOL_BASE_URL)
    }
}

fn parse_range_strings(value: &[String]) -> Result<(NaiveDate, NaiveDate), Error> {
    match value.len() {
        1 => {
            let value = NaiveDate::parse_from_str(&value[0], DATE_FORMAT)
                .map_err(|_| Error::InvalidDateRange(value.to_vec()))?;

            Ok((value, value))
        }
        2 => {
            let first = NaiveDate::parse_from_str(&value[0], DATE_FORMAT)
                .map_err(|_| Error::InvalidDateRange(value.to_vec()))?;
            let last = NaiveDate::parse_from_str(&value[1], DATE_FORMAT)
                .map_err(|_| Error::InvalidDateRange(value.to_vec()))?;

            Ok((first, last))
        }
        _ => Err(Error::InvalidDateRange(value.to_vec())),
    }
}

#[cfg(test)]
mod tests {
    use super::super::Observation;
    use super::*;
    use chrono::NaiveDate;

    #[tokio::test]
    async fn lookup_tw_screen_name() {
        let client = Client::default();

        let result = client.lookup_tw_screen_name("WLMact").await.unwrap();
        let expected = vec![(
            1470631321496084481,
            vec![
                Observation::new(
                    "i_am_not_a_nazi".to_string(),
                    Some((
                        NaiveDate::from_ymd(2022, 05, 19),
                        NaiveDate::from_ymd(2022, 06, 08),
                    )),
                ),
                Observation::new(
                    "WLMact".to_string(),
                    Some((
                        NaiveDate::from_ymd(2022, 06, 10),
                        NaiveDate::from_ymd(2022, 07, 05),
                    )),
                ),
            ],
        )]
        .into_iter()
        .collect();

        assert_eq!(result, expected);
    }
}
