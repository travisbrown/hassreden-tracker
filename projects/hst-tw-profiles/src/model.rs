use chrono::{DateTime, Utc};

#[derive(Debug, Default, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Url {
    pub url: String,
    pub expanded_url: Option<String>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub display_url: Option<String>,
    pub indices: Vec<i64>,
}

#[derive(Debug, Default, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Entity {
    pub urls: Vec<Url>,
}

#[derive(Debug, Default, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Entities {
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<Entity>,
    pub description: Option<Entity>,
}

#[derive(Debug, Default, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    pub id: i64,
    pub id_str: String,
    pub name: String,
    pub screen_name: String,
    pub location: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub entities: Option<Entities>,
    pub protected: bool,
    pub followers_count: i64,
    pub friends_count: i64,
    pub listed_count: i64,
    pub created_at: String,
    pub favourites_count: i64,
    pub utc_offset: Option<i32>,
    pub time_zone: Option<String>,
    pub geo_enabled: Option<bool>,
    pub verified: bool,
    pub statuses_count: i64,
    pub lang: Option<String>,
    pub profile_background_color: Option<String>,
    pub profile_background_image_url_https: Option<String>,
    pub profile_background_tile: Option<bool>,
    pub profile_image_url_https: String,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub profile_banner_url: Option<String>,
    pub profile_link_color: Option<String>,
    pub profile_sidebar_border_color: Option<String>,
    pub profile_sidebar_fill_color: Option<String>,
    pub profile_text_color: Option<String>,
    pub profile_use_background_image: Option<bool>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub has_extended_profile: Option<bool>,
    pub default_profile: bool,
    pub default_profile_image: bool,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub withheld_scope: Option<String>,
    pub withheld_in_countries: Vec<String>,
    pub snapshot: i64,
}

impl User {
    pub fn id(&self) -> u64 {
        self.id as u64
    }

    pub fn created_at(&self) -> Result<DateTime<Utc>, chrono::ParseError> {
        hst_tw_utils::parse_date_time(&self.created_at)
    }

    pub fn expanded_url(&self) -> Option<&str> {
        let entities = self.entities.as_ref()?;
        let entity = entities.url.as_ref()?;
        let first_url = entity.urls.first()?;
        first_url.expanded_url.as_deref()
    }

    pub fn description_urls(&self) -> Vec<&str> {
        self.entities
            .as_ref()
            .and_then(|entity| entity.description.as_ref())
            .map(|description| {
                description
                    .urls
                    .iter()
                    .filter_map(|url| url.expanded_url.as_deref())
                    .collect()
            })
            .unwrap_or_default()
    }
}
