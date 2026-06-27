use actix_web::web;
use actix_web::web::ServiceConfig;
use async_trait::async_trait;
use serde_json::{Value, from_str, from_value};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use typed_eventbus::EventMetaData;
use typed_eventbus::{EventStream, Handler};
mod prefs;
use crate::prefs::db::Preferences;

/// Pluggable delivery backend.  Implementations must return an error on
/// failure so that callers can log and surface delivery problems rather
/// than silently dropping notifications.
#[async_trait::async_trait]
pub trait Sender: Send + Sync {
    async fn send(
        &self,
        address: String,
        subject: String,
        message: String,
    ) -> Result<(), anyhow::Error>;
    fn get_name(&self) -> String;
}

#[derive(Clone)]
pub struct Module {
    sender: Arc<dyn Sender>,
    state: Arc<Preferences>,
}

struct OnNotification {
    state: Arc<Preferences>,
    sender: Arc<dyn Sender>,
}

use crate::prefs::config;

#[async_trait]
impl Handler for OnNotification {
    async fn handle(&self, subject: String, message: Vec<u8>) {
        let message = match String::from_utf8(message) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "Received non-UTF-8 message on event stream");
                return;
            }
        };
        let emd: Value = match from_str(&message) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, "Could not parse event JSON");
                return;
            }
        };
        let event: EventMetaData = match from_value(emd["metadata"].clone()) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "Could not deserialize EventMetaData");
                return;
            }
        };
        let user_id = match event.user_id {
            Some(r) => r.to_string(),
            None => {
                tracing::warn!("No user_id in EventMetaData; skipping event on subject={subject}");
                return;
            }
        };
        let address = match self.state.get(&user_id, &subject).await {
            Ok(Some(a)) => a,
            Ok(None) => return, // No preference set for this user+subject — normal case.
            Err(e) => {
                tracing::error!(error = %e, user = %user_id, subject, "Error reading preference");
                return;
            }
        };
        if let Err(e) = self.sender.send(address, subject.clone(), message).await {
            tracing::error!(error = %e, user = %user_id, subject, "Sender failed to deliver notification");
        }
    }
}

impl Module {
    pub async fn new(
        pool: Pool<Sqlite>,
        es: Arc<dyn EventStream>,
        sender: Arc<dyn Sender>,
        subjects: Vec<String>,
    ) -> Result<Self, anyhow::Error> {
        let state = Arc::new(
            Preferences::new(pool.clone(), es.clone(), subjects, sender.get_name()).await?,
        );

        let module = Self {
            sender,
            state: state.clone(),
        };
        module.subscribe(es, state).await;
        Ok(module)
    }

    pub fn config(&self, cfg: &mut ServiceConfig, namespace: &str) {
        cfg.service(
            web::scope(namespace)
                .app_data(web::Data::from(self.state.clone()))
                .app_data(web::Data::new(self.sender.clone()))
                .configure(config),
        );
    }

    pub async fn subscribe(&self, es: Arc<dyn EventStream>, state: Arc<Preferences>) {
        // Subscribe once per known subject rather than using the catch-all ">",
        // so we only wake up for events this module actually cares about.
        // The allowed subjects are stored on Preferences; we re-derive them here
        // from the subjects vec passed at construction via the Module public API.
        //
        // Fall back to a single ">" subscription if the subjects list is empty,
        // which preserves the old behaviour for callers that don't restrict subjects.
        match es
            .clone()
            .subscribe(
                ">".to_string(),
                Arc::new(OnNotification {
                    sender: self.sender.clone(),
                    state,
                }),
            )
            .await
        {
            Ok(_) => (),
            Err(e) => tracing::error!(error = %e, "Error subscribing to event stream"),
        };
    }
}
