use std::{collections::BTreeSet, sync::Arc};

use axum::{
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::config::AuthCredential;

#[derive(Debug, Clone)]
pub struct Actor {
    pub id: String,
    scopes: BTreeSet<String>,
}

impl Actor {
    #[must_use]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.contains(scope)
    }
}

#[derive(Clone)]
pub struct AuthRegistry {
    credentials: Arc<Vec<Credential>>,
}

struct Credential {
    actor: Actor,
    digest: [u8; 32],
}

impl AuthRegistry {
    pub fn new(config: &[AuthCredential]) -> anyhow::Result<Self> {
        let mut credentials = Vec::with_capacity(config.len());
        for entry in config {
            let bytes = hex::decode(&entry.token_sha256)?;
            let digest: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("token digest must be 32 bytes"))?;
            credentials.push(Credential {
                actor: Actor {
                    id: entry.actor_id.clone(),
                    scopes: entry.scopes.iter().cloned().collect(),
                },
                digest,
            });
        }
        Ok(Self {
            credentials: Arc::new(credentials),
        })
    }

    #[must_use]
    pub fn authenticate(&self, token: &str) -> Option<Actor> {
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut authenticated = None;
        for credential in self.credentials.iter() {
            if bool::from(credential.digest.ct_eq(&digest)) {
                authenticated = Some(credential.actor.clone());
            }
        }
        authenticated
    }
}

pub async fn require_auth(
    State(registry): State<AuthRegistry>,
    mut request: Request,
    next: Next,
) -> Response {
    let required_scope = required_scope(request.method(), request.uri().path());
    let Some(header) = request.headers().get(header::AUTHORIZATION) else {
        return AuthFailure::unauthorized().into_response();
    };
    let Ok(header) = header.to_str() else {
        return AuthFailure::unauthorized().into_response();
    };
    let Some(token) = header.strip_prefix("Bearer ") else {
        return AuthFailure::unauthorized().into_response();
    };
    let Some(actor) = registry.authenticate(token) else {
        return AuthFailure::unauthorized().into_response();
    };
    if !actor.has_scope(required_scope) {
        return AuthFailure::forbidden(required_scope).into_response();
    }
    request.extensions_mut().insert(actor);
    next.run(request).await
}

fn required_scope(method: &Method, path: &str) -> &'static str {
    if *method == Method::GET {
        return "read";
    }
    if path == "/v1/intents" {
        return "submit";
    }
    if path == "/v1/cancellations" {
        return "cancel";
    }
    "operate"
}

#[derive(Serialize)]
struct AuthFailure {
    error: AuthFailureBody,
}

#[derive(Serialize)]
struct AuthFailureBody {
    code: &'static str,
    message: String,
    retryable: bool,
    correlation_id: uuid::Uuid,
    details: Value,
}

impl AuthFailure {
    fn unauthorized() -> (StatusCode, axum::Json<Self>) {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(Self {
                error: AuthFailureBody {
                    code: "AUTH_REQUIRED",
                    message: "valid bearer authentication is required".into(),
                    retryable: false,
                    correlation_id: uuid::Uuid::now_v7(),
                    details: serde_json::json!({}),
                },
            }),
        )
    }

    fn forbidden(scope: &str) -> (StatusCode, axum::Json<Self>) {
        (
            StatusCode::FORBIDDEN,
            axum::Json(Self {
                error: AuthFailureBody {
                    code: "AUTH_SCOPE_REQUIRED",
                    message: format!("{scope} scope is required"),
                    retryable: false,
                    correlation_id: uuid::Uuid::now_v7(),
                    details: serde_json::json!({"required_scope": scope}),
                },
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::config::token_digest;

    use super::*;

    #[test]
    fn authenticates_without_exposing_token() {
        let registry = AuthRegistry::new(&[AuthCredential {
            actor_id: "strategy".into(),
            token_sha256: token_digest("a-high-entropy-test-token"),
            scopes: vec!["read".into()],
        }])
        .unwrap();
        assert_eq!(
            registry
                .authenticate("a-high-entropy-test-token")
                .unwrap()
                .id,
            "strategy"
        );
        assert!(registry.authenticate("wrong").is_none());
    }
}
