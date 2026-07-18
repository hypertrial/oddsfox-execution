use std::{fs, net::SocketAddr, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::Mode;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub server: ServerConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub paper: PaperConfig,
    #[serde(default)]
    pub polymarket: PolymarketConfig,
    pub auth: Vec<AuthCredential>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: SocketAddr,
    #[serde(default = "default_body_limit")]
    pub max_body_bytes: usize,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub allow_non_loopback: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            metrics_bind: default_metrics_bind(),
            max_body_bytes: default_body_limit(),
            request_timeout_ms: default_request_timeout_ms(),
            allow_non_loopback: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub database_path: String,
    #[serde(default = "default_backup_dir")]
    pub backup_dir: String,
    #[serde(default = "default_event_retention")]
    pub event_retention: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PaperConfig {
    #[serde(default = "default_quote_balance")]
    pub starting_quote_balance: String,
    #[serde(default)]
    pub starting_positions: Vec<PaperPosition>,
}

impl Default for PaperConfig {
    fn default() -> Self {
        Self {
            starting_quote_balance: default_quote_balance(),
            starting_positions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PaperPosition {
    pub condition_id: String,
    pub token_id: String,
    pub shares: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolymarketConfig {
    #[serde(default = "default_clob_url")]
    pub clob_url: String,
    #[serde(default = "default_websocket_url")]
    pub websocket_url: String,
    #[serde(default = "default_data_api_url")]
    pub data_api_url: String,
    #[serde(default = "default_geoblock_url")]
    pub geoblock_url: String,
    #[serde(default = "default_expected_protocol")]
    pub expected_protocol: u32,
    #[serde(default = "default_chain_id")]
    pub chain_id: u64,
    pub signer_address: Option<String>,
    pub funder_address: Option<String>,
    pub private_key_file: Option<String>,
    pub aws_kms_key_id: Option<String>,
    #[serde(default = "default_reconcile_seconds")]
    pub reconciliation_interval_seconds: u64,
    #[serde(default = "default_geoblock_seconds")]
    pub geoblock_interval_seconds: u64,
    #[serde(default = "default_heartbeat_seconds")]
    pub heartbeat_interval_seconds: u64,
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            clob_url: default_clob_url(),
            websocket_url: default_websocket_url(),
            data_api_url: default_data_api_url(),
            geoblock_url: default_geoblock_url(),
            expected_protocol: default_expected_protocol(),
            chain_id: default_chain_id(),
            signer_address: None,
            funder_address: None,
            private_key_file: None,
            aws_kms_key_id: None,
            reconciliation_interval_seconds: default_reconcile_seconds(),
            geoblock_interval_seconds: default_geoblock_seconds(),
            heartbeat_interval_seconds: default_heartbeat_seconds(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthCredential {
    pub actor_id: String,
    pub token_sha256: String,
    pub scopes: Vec<String>,
}

impl std::fmt::Debug for AuthCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthCredential")
            .field("actor_id", &self.actor_id)
            .field("token_sha256", &"[REDACTED]")
            .field("scopes", &self.scopes)
            .finish()
    }
}

impl Config {
    pub fn load(path: &Path, mode_override: Option<Mode>) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("read configuration {}", path.display()))?;
        let mut config: Self =
            toml::from_str(&contents).context("parse strict TOML configuration")?;
        if let Some(mode) = mode_override {
            config.mode = mode;
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_auth()?;
        self.validate_runtime_bounds()?;
        if self.mode == Mode::Live {
            self.validate_live()?;
        }
        Ok(())
    }

    fn validate_auth(&self) -> Result<()> {
        if self.auth.is_empty() {
            bail!("at least one bearer credential is required");
        }
        let mut actor_ids = std::collections::BTreeSet::new();
        let mut token_digests = std::collections::BTreeSet::new();
        for credential in &self.auth {
            if credential.actor_id.trim().is_empty() {
                bail!("auth actor_id may not be empty");
            }
            if !actor_ids.insert(&credential.actor_id) {
                bail!("auth actor_id values must be unique");
            }
            let digest = hex::decode(&credential.token_sha256)
                .context("auth token_sha256 must be hexadecimal")?;
            let digest: [u8; 32] = digest
                .try_into()
                .map_err(|_| anyhow::anyhow!("auth token_sha256 must contain a SHA-256 digest"))?;
            if !token_digests.insert(digest) {
                bail!("auth token digests must be unique");
            }
            if credential.scopes.is_empty() {
                bail!("each auth credential requires at least one scope");
            }
            for scope in &credential.scopes {
                if !matches!(scope.as_str(), "read" | "submit" | "cancel" | "operate") {
                    bail!("unsupported auth scope {scope}");
                }
            }
        }
        Ok(())
    }

    fn validate_runtime_bounds(&self) -> Result<()> {
        if self.server.max_body_bytes == 0 || self.server.request_timeout_ms == 0 {
            bail!("server body and timeout bounds must be positive");
        }
        if self.storage.event_retention == 0 {
            bail!("storage.event_retention must be positive");
        }
        if !(1..=60).contains(&self.polymarket.reconciliation_interval_seconds) {
            bail!("reconciliation_interval_seconds must be between 1 and 60");
        }
        if !(1..=300).contains(&self.polymarket.geoblock_interval_seconds) {
            bail!("geoblock_interval_seconds must be between 1 and 300");
        }
        if !(1..=5).contains(&self.polymarket.heartbeat_interval_seconds) {
            bail!("heartbeat_interval_seconds must be between 1 and 5");
        }
        Ok(())
    }

    fn validate_live(&self) -> Result<()> {
        if !cfg!(feature = "live") {
            bail!("live mode requires a binary built with the live feature");
        }
        if std::env::var("ODDSFOX_ENABLE_LIVE_TRADING").as_deref() != Ok("YES") {
            bail!("live mode requires ODDSFOX_ENABLE_LIVE_TRADING=YES");
        }
        for (name, address) in [
            (
                "polymarket.signer_address",
                self.polymarket.signer_address.as_deref(),
            ),
            (
                "polymarket.funder_address",
                self.polymarket.funder_address.as_deref(),
            ),
        ] {
            let Some(address) = address else {
                bail!("live mode requires {name}");
            };
            if !valid_evm_address(address) {
                bail!("{name} must be a nonzero 20-byte hexadecimal address");
            }
        }
        if self
            .auth
            .iter()
            .any(|credential| credential.token_sha256.bytes().all(|byte| byte == b'0'))
        {
            bail!("live mode refuses placeholder bearer-token digests");
        }
        if self.polymarket.chain_id != 137 || self.polymarket.expected_protocol != 2 {
            bail!("live mode supports only Polygon chain 137 and Polymarket protocol V2");
        }
        if !self.polymarket.clob_url.starts_with("https://")
            || !self.polymarket.data_api_url.starts_with("https://")
            || !self.polymarket.geoblock_url.starts_with("https://")
            || !self.polymarket.websocket_url.starts_with("wss://")
        {
            bail!("live venue endpoints require HTTPS/WSS");
        }
        match (
            self.polymarket.private_key_file.is_some(),
            self.polymarket.aws_kms_key_id.is_some(),
        ) {
            (false, false) => bail!("live mode requires a mounted key file or AWS KMS key"),
            (true, true) => bail!("configure exactly one live signer: mounted key file or AWS KMS"),
            (false, true) if !cfg!(feature = "aws-kms") => {
                bail!("AWS KMS requires a binary built with the aws-kms feature")
            }
            _ => {}
        }
        if !self.server.bind.ip().is_loopback() && !self.server.allow_non_loopback {
            bail!("live non-loopback HTTP bind requires server.allow_non_loopback=true");
        }
        if !self.server.metrics_bind.ip().is_loopback() {
            bail!("live metrics listener must bind to loopback");
        }
        Ok(())
    }

    #[must_use]
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.server.request_timeout_ms)
    }
}

#[must_use]
pub fn token_digest(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn default_bind() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8787))
}

fn default_metrics_bind() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 9090))
}

const fn default_body_limit() -> usize {
    64 * 1024
}

const fn default_request_timeout_ms() -> u64 {
    45_000
}

fn default_backup_dir() -> String {
    "./data/backups".into()
}

const fn default_event_retention() -> u64 {
    1_000_000
}

fn default_quote_balance() -> String {
    "10000".into()
}

fn default_clob_url() -> String {
    "https://clob.polymarket.com".into()
}

fn default_websocket_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com".into()
}

fn default_data_api_url() -> String {
    "https://data-api.polymarket.com".into()
}

fn default_geoblock_url() -> String {
    "https://polymarket.com/api/geoblock".into()
}

const fn default_expected_protocol() -> u32 {
    2
}

const fn default_chain_id() -> u64 {
    137
}

const fn default_reconcile_seconds() -> u64 {
    60
}

const fn default_geoblock_seconds() -> u64 {
    300
}

const fn default_heartbeat_seconds() -> u64 {
    5
}

fn valid_evm_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        && value[2..].bytes().any(|byte| byte != b'0')
}

impl Serialize for Config {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Redacted<'a> {
            mode: Mode,
            bind: SocketAddr,
            database_path: &'a str,
            clob_url: &'a str,
            auth_actors: Vec<&'a str>,
        }
        Redacted {
            mode: self.mode,
            bind: self.server.bind,
            database_path: &self.storage.database_path,
            clob_url: &self.polymarket.clob_url,
            auth_actors: self
                .auth
                .iter()
                .map(|entry| entry.actor_id.as_str())
                .collect(),
        }
        .serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(auth: Vec<AuthCredential>) -> Config {
        Config {
            mode: Mode::Paper,
            server: ServerConfig::default(),
            storage: StorageConfig {
                database_path: "test.sqlite3".into(),
                backup_dir: "backups".into(),
                event_retention: 100,
            },
            paper: PaperConfig::default(),
            polymarket: PolymarketConfig::default(),
            auth,
        }
    }

    #[test]
    fn auth_digest_uniqueness_is_based_on_decoded_bytes() {
        let digest = "ab".repeat(32);
        let config = test_config(vec![
            AuthCredential {
                actor_id: "first".into(),
                token_sha256: digest.clone(),
                scopes: vec!["read".into()],
            },
            AuthCredential {
                actor_id: "second".into(),
                token_sha256: digest.to_ascii_uppercase(),
                scopes: vec!["read".into()],
            },
        ]);

        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("token digests must be unique")
        );
    }
}
