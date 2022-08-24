use crate::age::ProfileAgeDb;
use egg_mode_extras::client::{Client, FormerUserStatus, TokenType};
use hst_deactivations::file::DeactivationFile;
use std::sync::Arc;

pub struct Downloader {
    twitter_client: Arc<Client>,
    deactivations: DeactivationFile,
    profile_age_db: ProfileAgeDb,
}

impl Downloader {
    pub fn new(
        twitter_client: Arc<Client>,
        deactivations: DeactivationFile,
        profile_age_db: ProfileAgeDb,
    ) -> Self {
        Self {
            twitter_client,
            deactivations,
            profile_age_db,
        }
    }
}
