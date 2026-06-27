use actix_web::web;
use actix_web::web::ServiceConfig;
use async_trait::async_trait;
use typed_eventbus::EventMetaData;
use typed_eventbus::{EventStream, Handler};
use serde_json::{Value, from_str, from_value};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
mod prefs;
use crate::prefs::db::Preferences;

#[async_trait::async_trait]
pub trait Sender: Send + Sync {
    async fn send(&self, address: String, subject: String, message: String);
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
        let message = String::from_utf8(message).unwrap();
        let emd = from_str::<Value>(&message).unwrap();
        let event: EventMetaData = match from_value(emd["metadata"].clone()) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Could not parse json: {e}");
                println!("message received: {message}");
                return ();
            }
        };
        let user_id = match event.user_id {
            Some(r) => r.to_string(),
            None => {
                eprintln!("No user id in Event Metadata");
                return;
            }
        };
        let address = match self.state.get(&user_id, &subject).await {
            Ok(r) => match r {
                Some(x) => x,
                None => return (),
            },
            Err(e) => {
                eprintln!("Error in reading preferences: {e}");
                return;
            }
        };
        self.sender.send(address, subject, message).await;
    }
}

impl Module {
    pub async fn new(
        pool: Pool<Sqlite>,
        es: Arc<dyn EventStream>,
        sender: Arc<dyn Sender>,
        subjects: Vec<String>,
    ) -> Self {
        let state = Arc::new(Preferences::new(pool.clone(), es.clone(), subjects, sender.get_name()).await);

        let module = Self {
            sender,
            state: state.clone(),
        };
        module.subscribe(es, state).await;
        module
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
            Err(e) => eprintln!("Error in subscribing to event stream: {e}"),
        };
    }
}
