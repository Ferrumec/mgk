use anyhow::Result;
use moka::future::Cache;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::{collections::HashSet, sync::Arc};
use typed_eventbus::{Event, EventMetaData, EventStream, Publishable};
use validator::Validate;

fn gen_otp() -> u32 {
    let mut rng = rand::rng();
    rng.random_range(100000..999999)
}

// Models

#[derive(Deserialize, Validate, Clone)]
pub struct Preference {
    #[validate(length(max = 64))]
    pub subject: String,
    #[validate(length(max = 64))]
    pub address: String,
}

#[derive(Deserialize, Validate)]
pub struct Token {
    #[validate(range(min = 100000, max = 999999))]
    pub token: u32,
}

// ---------------------------------------------------------------------------
// Preferences
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Preferences {
    db: Pool<Sqlite>,
    cache: Cache<(String, String), String>, // (user, subject) -> Channel
    pending: Cache<(String, u32), (String, String)>,
    allowed_subjects: HashSet<String>,
    table_name: String,
    es: Arc<dyn EventStream>,
}

impl Preferences {
    pub async fn new(
        db: Pool<Sqlite>,
        es: Arc<dyn EventStream>,
        subjects: Vec<String>,
        sender_name: String,
    ) -> Self {
        let table_name = format!("{}_preferences", sender_name);
        init_table(&db, &table_name)
            .await
            .expect("could not initialize table");
        Self {
            db,
            es,
            table_name,
            cache: Cache::new(1000),
            pending: Cache::new(100),
            allowed_subjects: subjects.into_iter().collect(),
        }
    }

    pub async fn confirm(&self, user: &str, otp: &Token) -> Result<()> {
        if let Err(e) = otp.validate() {
            return Err(anyhow::anyhow!("invalid token: {e}"));
        }
        let (subject, channel) = match self.pending.remove(&(user.to_string(), otp.token)).await {
            Some(r) => r,
            None => return Err(anyhow::anyhow!("Token not found")),
        };
        sqlx::query(&format!(
            "INSERT INTO {} (user, subject, address)
             VALUES (?, ?, ?)
             ON CONFLICT(user, subject)
             DO UPDATE SET address = excluded.address",
            self.table_name
        ))
        .bind(&user)
        .bind(&subject)
        .bind(&channel)
        .execute(&self.db)
        .await?;

        self.cache
            .insert((user.to_string(), subject.to_string()), channel.clone())
            .await;
        let event = ChannelConfirmed {
            user: user.to_string(),
            subject,
            address: channel,
        };
        let emd = EventMetaData::new("mgk");
        let event = Event::new(emd, event);
        let _ = event.publish(self.es.clone()).await;
        Ok(())
    }

    pub async fn get(&self, user: &str, subject: &str) -> Result<Option<String>> {
        let key = (user.to_string(), subject.to_string());

        if let Some(cached) = self.cache.get(&key).await {
            return Ok(Some(cached));
        }

        let result = sqlx::query_scalar::<_, String>(&format!(
            "SELECT address FROM {} WHERE user = ? AND subject = ?",
            self.table_name
        ))
        .bind(user)
        .bind(subject)
        .fetch_optional(&self.db)
        .await?;

        if let Some(channel) = result {
            self.cache.insert(key, channel.clone()).await;
            return Ok(Some(channel));
        }
        Ok(None)
    }

    pub async fn set(&self, user: &str, pref: Preference) -> Result<u32> {
        if let Err(e) = pref.validate() {
            return Err(anyhow::anyhow!("Invalid data: {e}"));
        }
        if !self.allowed_subjects.contains(&pref.subject) {
            return Err(anyhow::anyhow!("Subject not allowed"));
        }
        let otp = gen_otp();
        self.pending
            .insert((user.into(), otp), (pref.subject, pref.address))
            .await;
        Ok(otp)
    }
}

async fn init_table(pool: &Pool<Sqlite>, table_name: &String) -> Result<(), sqlx::Error> {
    if let Err(e) = sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {} (
user TEXT,
subject TEXT,
address TEXT,
UNIQUE(user, subject)
)
",
        table_name
    ))
    .execute(pool)
    .await
    {
        Err(e)
    } else {
        Ok(())
    }
}

#[derive(Serialize)]
struct ChannelConfirmed {
    user: String,
    subject: String,
    address: String,
}

impl Publishable for ChannelConfirmed {
    const SUBJECT: &'static str = "contact.channel.confirmed";
}
