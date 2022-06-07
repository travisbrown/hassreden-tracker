use chrono::{DateTime, Utc};

const TWITTER_DATE_TIME_FMT: &str = "%a %b %d %H:%M:%S %z %Y";

pub fn parse_date_time(input: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_str(input, TWITTER_DATE_TIME_FMT)?.into())
}
