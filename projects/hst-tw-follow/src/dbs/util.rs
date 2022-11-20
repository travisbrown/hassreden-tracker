use chrono::{DateTime, Duration, TimeZone, Utc};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, Value, ValueRef};
use rusqlite::Result;

pub(crate) struct SQLiteId(pub(crate) u64);

impl ToSql for SQLiteId {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(self.0 as i64)))
    }
}

impl FromSql for SQLiteId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let id: i64 = FromSql::column_result(value)?;

        Ok(SQLiteId(id as u64))
    }
}

pub(crate) struct SQLiteUtc(pub(crate) DateTime<Utc>);

impl ToSql for SQLiteUtc {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(self.0.timestamp())))
    }
}

impl FromSql for SQLiteUtc {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let timestamp: i64 = FromSql::column_result(value)?;
        let utc = Utc
            .timestamp_opt(timestamp, 0)
            .single()
            .ok_or(FromSqlError::InvalidType)?;
        Ok(SQLiteUtc(utc))
    }
}

pub(crate) struct SQLiteDuration(pub(crate) Duration);

impl ToSql for SQLiteDuration {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(self.0.num_seconds())))
    }
}

impl FromSql for SQLiteDuration {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let seconds: i64 = FromSql::column_result(value)?;

        Ok(SQLiteDuration(Duration::seconds(seconds)))
    }
}
