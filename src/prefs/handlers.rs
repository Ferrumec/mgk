use std::sync::Arc;

use crate::Sender;
use crate::prefs::db::{PreferenceBatch, Token};

use super::db::Preferences;
use actix_web::web;
use actix_web::{HttpResponse, Responder};
use actixutils::{Auth, Identity};
use serde::{Deserialize, Serialize};
use tracing::error;

// ---------------------------------------------------------------------------
// Preferences
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct PreferenceGetQuery {
    pub subject: String,
}

/// Response returned from POST /preferences/set so the client can pass the
/// nonce back to POST /preferences/confirm alongside the OTP.
#[derive(Serialize)]
struct SetResponse {
    nonce: String,
}

/// Body expected by POST /preferences/confirm.
#[derive(Deserialize)]
pub struct ConfirmBody {
    pub nonce: String,
    pub token: u32,
}

pub async fn set_preference(
    Auth(id): Auth<Identity>,
    state: web::Data<Preferences>,
    sender: web::Data<Arc<dyn Sender>>,
    body: web::Json<PreferenceBatch>,
) -> impl Responder {
    let batch = body.into_inner();

    // Capture the address before we move the batch into `set`.
    let address = match batch.preferences.first() {
        Some(p) => p.address.clone(),
        None => return HttpResponse::BadRequest().body("preferences must not be empty"),
    };

    match state.set(&id.sub.to_string(), batch).await {
        Ok((nonce, otp)) => {
            // Send the OTP to the user's address out-of-band.
            // Log failures rather than silently swallowing them; the HTTP
            // response is still 200 because the pending entry was recorded
            // and the user may retry via /confirm within the TTL window.
            let result = sender
                .send(
                    address.clone(),
                    "confirm address".to_owned(),
                    otp.to_string(),
                )
                .await;
            if let Err(e) = result {
                error!(
                    error = %e,
                    user = %id.sub,
                    address = %address,
                    "Failed to deliver OTP; pending entry still recorded"
                );
            }
            HttpResponse::Ok().json(SetResponse { nonce })
        }
        Err(e) => {
            error!(
                error = %e,
                user = %id.sub,
                "Failed to set user preference batch"
            );
            HttpResponse::Forbidden().body(e.to_string())
        }
    }
}

pub async fn confirm_preference(
    Auth(id): Auth<Identity>,
    state: web::Data<Preferences>,
    body: web::Json<ConfirmBody>,
) -> impl Responder {
    let body = body.into_inner();
    let token = Token { token: body.token };
    match state
        .confirm(&id.sub.to_string(), &body.nonce, &token)
        .await
    {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => {
            error!(
                error = %e,
                user = %id.sub,
                "Failed to confirm user preference"
            );
            HttpResponse::InternalServerError().body(e.to_string())
        }
    }
}

pub async fn get_preference(
    Auth(id): Auth<Identity>,
    state: web::Data<Preferences>,
    query: web::Query<PreferenceGetQuery>,
) -> impl Responder {
    match state.get(&id.sub.to_string(), &query.subject).await {
        Ok(Some(channel)) => HttpResponse::Ok().json(channel),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(e) => {
            error!(
                error = %e,
                user = %id.sub,
                subject = %query.subject,
                "Failed to get user preference"
            );
            HttpResponse::InternalServerError().body(e.to_string())
        }
    }
}
