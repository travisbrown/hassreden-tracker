use chrono::{DateTime, TimeZone, Utc};

const TWITTER_DATE_TIME_FMT: &str = "%a %b %d %H:%M:%S %z %Y";

/// Parse the time format used in Twitter API responses.
pub fn parse_date_time(input: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_str(input, TWITTER_DATE_TIME_FMT)?.into())
}

const FIRST_SNOWFLAKE: i64 = 250000000000000;

fn is_snowflake(value: i64) -> bool {
    value >= FIRST_SNOWFLAKE
}

pub fn snowflake_to_date_time(value: i64) -> Option<DateTime<Utc>> {
    if is_snowflake(value) {
        let timestamp_millis = (value >> 22) + 1288834974657;

        Utc.timestamp_millis_opt(timestamp_millis).single()
    } else {
        None
    }
}
