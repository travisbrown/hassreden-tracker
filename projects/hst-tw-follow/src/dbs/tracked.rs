use super::util::{SQLiteDuration, SQLiteId};
use chrono::Duration;
use hst_tw_db::ProfileDb;
use rusqlite::{params, Connection, OptionalExtension, Row};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackedUser {
    pub id: u64,
    screen_name: String,
    pub target_age: Option<Duration>,
    pub followers_count: usize,
    pub protected: bool,
    pub blocks: HashSet<u64>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("SQLite error")]
    Db(#[from] rusqlite::Error),
    #[error("ProfileDB error")]
    ProfileDb(#[from] hst_tw_db::Error),
}

const ID_SELECT: &str = "SELECT id FROM user ORDER BY id";

const USER_SELECT: &str = "
    SELECT id, screen_name, target_age, followers_count, protected
        FROM user WHERE id = ?
";

const USER_SELECT_ALL: &str =
    "SELECT id, screen_name, target_age, followers_count, protected FROM user ORDER BY id";

const USER_UPDATE_PROTECTED: &str = "UPDATE user SET protected = ? WHERE id = ?";

const USER_UPSERT: &str = "
    INSERT INTO user (id, screen_name, followers_count, protected)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
          screen_name = excluded.screen_name,
          followers_count = excluded.followers_count,
          protected = excluded.protected
";

const BLOCK_SELECT: &str = "SELECT target_id FROM block WHERE id = ?";
const BLOCK_SELECT_ALL: &str = "SELECT id, target_id FROM block ORDER BY id, target_id";
const BLOCK_INSERT: &str = "INSERT OR IGNORE INTO block (id, target_id) VALUES (?, ?)";

#[derive(Clone)]
pub struct TrackedUserDb {
    connection: Arc<RwLock<Connection>>,
}

impl TrackedUserDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let exists = path.as_ref().is_file();
        let connection = Connection::open(path)?;

        if !exists {
            let schema = std::include_str!("../schemas/tracked.sql");
            connection.execute_batch(schema)?;
        }

        Ok(Self {
            connection: Arc::new(RwLock::new(connection)),
        })
    }

    pub fn update_all<M>(
        &self,
        profile_db: &ProfileDb<M>,
        ids: Option<HashSet<u64>>,
    ) -> Result<Vec<u64>, Error> {
        let ids = match ids {
            Some(ids) => ids,
            None => self.ids()?.into_iter().collect(),
        };
        let mut not_found = vec![];

        for id in ids {
            match profile_db.lookup_latest(id)? {
                Some((_, profile)) => {
                    let user = TrackedUser {
                        id: profile.id(),
                        screen_name: profile.screen_name,
                        followers_count: profile.followers_count as usize,
                        target_age: None,
                        protected: profile.protected,
                        blocks: HashSet::new(),
                    };

                    self.put(&user)?;
                }
                None => {
                    not_found.push(id);
                }
            }
        }

        not_found.sort_unstable();

        Ok(not_found)
    }

    pub fn put(&self, user: &TrackedUser) -> Result<(), Error> {
        let connection = self.connection.read().unwrap();
        let mut upsert = connection.prepare_cached(USER_UPSERT)?;

        upsert.execute(params![
            SQLiteId(user.id),
            user.screen_name,
            user.followers_count,
            user.protected
        ])?;

        for target_id in &user.blocks {
            self.put_block(user.id, *target_id)?
        }

        Ok(())
    }

    pub fn put_block(&self, id: u64, target_id: u64) -> Result<(), Error> {
        let connection = self.connection.read().unwrap();
        let mut insert = connection.prepare_cached(BLOCK_INSERT)?;

        insert.execute(params![SQLiteId(id), SQLiteId(target_id),])?;

        Ok(())
    }

    pub fn set_protected(&self, id: u64, protected: bool) -> Result<(), Error> {
        let connection = self.connection.read().unwrap();
        let mut update = connection.prepare_cached(USER_UPDATE_PROTECTED)?;

        update.execute(params![SQLiteId(id), protected])?;

        Ok(())
    }

    pub fn get(&self, id: u64) -> Result<Option<TrackedUser>, Error> {
        let connection = self.connection.read().unwrap();
        let mut block_select = connection.prepare_cached(BLOCK_SELECT)?;

        let blocks = block_select
            .query_map(params![SQLiteId(id)], |row| {
                let id: SQLiteId = row.get(0)?;
                Ok(id.0)
            })?
            .collect::<Result<HashSet<_>, _>>()?;

        let mut select = connection.prepare_cached(USER_SELECT)?;

        let mut user: Option<TrackedUser> = select
            .query_row(params![SQLiteId(id)], row_to_user)
            .optional()?;

        if let Some(ref mut user) = user {
            user.blocks = blocks;
        }

        Ok(user)
    }

    pub fn ids(&self) -> Result<Vec<u64>, Error> {
        let connection = self.connection.read().unwrap();
        let mut select = connection.prepare_cached(ID_SELECT)?;

        let ids = select
            .query_map([], |row| {
                let id: SQLiteId = row.get(0)?;
                Ok(id.0)
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ids)
    }

    pub fn users(&self) -> Result<Vec<TrackedUser>, Error> {
        let connection = self.connection.read().unwrap();
        let mut block_select_all = connection.prepare_cached(BLOCK_SELECT_ALL)?;

        let block_pairs = block_select_all
            .query_map([], |row| {
                let id: SQLiteId = row.get(0)?;
                let target_id: SQLiteId = row.get(1)?;
                Ok((id.0, target_id.0))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut blocks: HashMap<u64, HashSet<u64>> = HashMap::new();

        for (id, target_id) in block_pairs {
            blocks.entry(id).or_default().insert(target_id);
        }

        let mut select_all = connection.prepare_cached(USER_SELECT_ALL)?;

        let mut users = select_all
            .query_map(params![], row_to_user)?
            .collect::<Result<Vec<_>, _>>()?;

        for mut user in &mut users {
            if let Some(blocks) = blocks.remove(&user.id) {
                user.blocks = blocks;
            }
        }

        Ok(users)
    }

    pub fn export(&self) -> Result<Vec<(u64, String, Option<Duration>)>, Error> {
        let users = self.users()?;

        Ok(users
            .into_iter()
            .map(|user| (user.id, user.screen_name, user.target_age))
            .collect())
    }
}

fn row_to_user(row: &Row) -> Result<TrackedUser, rusqlite::Error> {
    let id: SQLiteId = row.get(0)?;
    let target_age: Option<SQLiteDuration> = row.get(2)?;

    Ok(TrackedUser {
        id: id.0,
        screen_name: row.get(1)?,
        target_age: target_age.map(|wrapped| wrapped.0),
        followers_count: row.get(3)?,
        protected: row.get(4)?,
        blocks: HashSet::new(),
    })
}
