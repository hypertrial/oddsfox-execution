use std::{
    io::Read as _,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use metrics_exporter_prometheus::PrometheusBuilder;
use oddsfox_execution::{
    Config,
    api::{ApiState, metrics_router, openapi_document, router},
    auth::AuthRegistry,
    config::token_digest,
    domain::{CancellationRequest, Mode, OrderIntentRequest, ReasonRequest},
    execution::ExecutionCoordinator,
    risk::RiskPolicy,
    store::{Store, verify_backup_offline},
    venue::{ExecutionVenue, PaperVenue, PolymarketVenue, validate_live_signer},
};
use reqwest::{Client, Method};
use secrecy::{ExposeSecret as _, ExposeSecretMut as _, SecretBox, SecretString};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "oddsfox-exec", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Capabilities,
    Serve(ServeArgs),
    Doctor(DoctorArgs),
    Submit(RequestFileArgs),
    Cancel(CancelArgs),
    Halt(ReasonArgs),
    Resume(ReasonArgs),
    Orders(ApiArgs),
    Trades(ApiArgs),
    Positions(ApiArgs),
    Reconcile(ApiArgs),
    Backup(ApiArgs),
    TokenDigest,
    Openapi {
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        check: Option<PathBuf>,
    },
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, default_value = "config/oddsfox.toml")]
    config: PathBuf,
    #[arg(long, default_value = "config/risk-policy.json")]
    risk_policy: PathBuf,
    #[arg(long)]
    mode: Option<String>,
}

#[derive(Args)]
struct DoctorArgs {
    #[arg(long, default_value = "config/oddsfox.toml")]
    config: PathBuf,
    #[arg(long, default_value = "config/risk-policy.json")]
    risk_policy: PathBuf,
    #[arg(long)]
    backup: Option<PathBuf>,
}

#[derive(Args)]
struct RequestFileArgs {
    #[command(flatten)]
    api: ApiArgs,
    #[arg(long)]
    request: PathBuf,
}

#[derive(Args)]
struct CancelArgs {
    #[command(flatten)]
    api: ApiArgs,
    #[arg(long)]
    order_id: Option<Uuid>,
    #[arg(long)]
    intent_id: Option<Uuid>,
    #[arg(long)]
    condition_id: Option<String>,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    reason: String,
}

#[derive(Args)]
struct ReasonArgs {
    #[command(flatten)]
    api: ApiArgs,
    #[arg(long)]
    reason: String,
}

#[derive(Clone, Args)]
struct ApiArgs {
    #[arg(long, default_value = "http://127.0.0.1:8787")]
    url: String,
    #[arg(long, env = "ODDSFOX_API_TOKEN_FILE", hide_env_values = true)]
    token_file: PathBuf,
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct Capabilities {
    schema_version: &'static str,
    modes: &'static [&'static str],
    signer: Option<&'static str>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    match Cli::parse().command {
        Command::Capabilities => {
            println!("{}", serde_json::to_string(&capabilities())?);
            Ok(())
        }
        Command::Serve(args) => serve(args).await,
        Command::Doctor(args) => doctor(&args).await,
        Command::Submit(args) => {
            let request: OrderIntentRequest =
                serde_json::from_slice(&std::fs::read(&args.request)?)?;
            print_json(
                api_request(&args.api, Method::POST, "/v1/intents", Some(&request), true).await?,
            )
        }
        Command::Cancel(args) => {
            let request = CancellationRequest {
                order_id: args.order_id,
                intent_id: args.intent_id,
                condition_id: args.condition_id,
                all_open_orders: args.all,
                reason: args.reason,
            };
            request.validate().map_err(|error| anyhow::anyhow!(error))?;
            print_json(
                api_request(
                    &args.api,
                    Method::POST,
                    "/v1/cancellations",
                    Some(&request),
                    true,
                )
                .await?,
            )
        }
        Command::Halt(args) => control_request(&args.api, "/v1/control/halt", &args.reason).await,
        Command::Resume(args) => {
            control_request(&args.api, "/v1/control/resume", &args.reason).await
        }
        Command::Orders(api) => {
            print_json(api_request::<Value>(&api, Method::GET, "/v1/orders", None, false).await?)
        }
        Command::Trades(api) => {
            print_json(api_request::<Value>(&api, Method::GET, "/v1/trades", None, false).await?)
        }
        Command::Positions(api) => {
            print_json(api_request::<Value>(&api, Method::GET, "/v1/positions", None, false).await?)
        }
        Command::Reconcile(api) => print_json(
            api_request::<Value>(&api, Method::POST, "/v1/reconciliations", None, true).await?,
        ),
        Command::Backup(api) => {
            print_json(api_request::<Value>(&api, Method::POST, "/v1/backups", None, true).await?)
        }
        Command::TokenDigest => {
            let mut token = String::new();
            std::io::stdin().read_to_string(&mut token)?;
            let token = token.trim_end_matches(['\r', '\n']);
            anyhow::ensure!(
                token.len() >= 32,
                "bearer token input must contain at least 32 characters"
            );
            println!("{}", token_digest(token));
            Ok(())
        }
        Command::Openapi { output, check } => {
            let document = openapi_document();
            if let Some(path) = &check {
                let checked: Value = serde_json::from_slice(&std::fs::read(path)?)?;
                anyhow::ensure!(
                    checked == document,
                    "checked-in OpenAPI differs from generated contract: {}",
                    path.display()
                );
            }
            if let Some(path) = output {
                std::fs::write(path, serde_json::to_vec_pretty(&document)?)?;
            } else if check.is_none() {
                println!("{}", serde_json::to_string_pretty(&document)?);
            }
            Ok(())
        }
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let mode_override = args.mode.as_deref().map(Mode::from_str).transpose()?;
    let config = Config::load(&args.config, mode_override)?;
    let policy = RiskPolicy::load(&args.risk_policy)?;
    validate_mode_policy(&config, &policy)?;
    let auth = AuthRegistry::new(&config.auth)?;
    let recorder = PrometheusBuilder::new().install_recorder()?;
    let listener = TcpListener::bind(config.server.bind)
        .await
        .with_context(|| format!("bind control API {}", config.server.bind))?;
    let metrics_listener = TcpListener::bind(config.server.metrics_bind)
        .await
        .with_context(|| format!("bind metrics API {}", config.server.metrics_bind))?;
    let store = Store::open(&config.storage, config.mode, &config.polymarket).await?;
    let venue: Arc<dyn ExecutionVenue> = match config.mode {
        Mode::Paper => Arc::new(
            PaperVenue::new(
                &config.paper,
                &config.polymarket.clob_url,
                &config.polymarket.websocket_url,
                store.clone(),
            )
            .await?,
        ),
        Mode::Live => Arc::new(PolymarketVenue::new(config.polymarket.clone()).await?),
    };
    let (coordinator, tasks) = ExecutionCoordinator::start(
        store.clone(),
        venue,
        policy.clone(),
        Duration::from_secs(config.polymarket.reconciliation_interval_seconds),
        Duration::from_secs(config.polymarket.heartbeat_interval_seconds),
    );
    coordinator.startup().await?;

    let app = router(
        ApiState::new(store.clone(), coordinator.clone()),
        auth,
        config.server.max_body_bytes,
        config.request_timeout(),
    );
    let metrics_app = metrics_router(recorder);
    info!(
        mode = %config.mode,
        bind = %config.server.bind,
        metrics_bind = %config.server.metrics_bind,
        "oddsfox execution ready"
    );
    let metrics_server = tokio::spawn(async move {
        if let Err(error) = axum::serve(metrics_listener, metrics_app).await {
            error!(%error, "metrics server stopped");
        }
    });
    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());
    let result = server.await;
    coordinator.shutdown(policy.cancel_on_halt).await?;
    tasks.shutdown().await;
    metrics_server.abort();
    store.close().await;
    result.context("control API server")
}

async fn doctor(args: &DoctorArgs) -> Result<()> {
    if let Some(manifest) = &args.backup {
        let verified = verify_backup_offline(manifest).await?;
        println!("{}", serde_json::to_string_pretty(&verified)?);
        return Ok(());
    }
    let config = Config::load(&args.config, None)?;
    let policy = RiskPolicy::load(&args.risk_policy)?;
    validate_mode_policy(&config, &policy)?;
    let database_parent = Path::new(&config.storage.database_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    anyhow::ensure!(
        database_parent.exists(),
        "database parent does not exist: {}",
        database_parent.display()
    );
    if config.mode == Mode::Live {
        validate_live_signer(&config.polymarket)?;
    }
    let venue_check = doctor_public_venue(&config).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "status": "ok",
            "mode": config.mode,
            "database_path": config.storage.database_path,
            "clob_url": config.polymarket.clob_url,
            "risk_policy_version": policy.version,
            "venue": venue_check,
        }))?
    );
    Ok(())
}

async fn doctor_public_venue(config: &Config) -> Result<Value> {
    let client = Client::builder()
        .timeout(config.request_timeout())
        .build()?;
    let clob_url = config.polymarket.clob_url.trim_end_matches('/');
    let version: u32 = client
        .get(format!("{clob_url}/version"))
        .send()
        .await
        .context("query Polymarket protocol version")?
        .error_for_status()
        .context("Polymarket version endpoint rejected the request")?
        .json()
        .await
        .context("parse Polymarket protocol version")?;
    anyhow::ensure!(
        version == config.polymarket.expected_protocol,
        "expected Polymarket protocol {}, venue reports {version}",
        config.polymarket.expected_protocol
    );
    let server_time_ms: i64 = client
        .get(format!("{clob_url}/time"))
        .send()
        .await
        .context("query Polymarket server time")?
        .error_for_status()
        .context("Polymarket time endpoint rejected the request")?
        .json()
        .await
        .context("parse Polymarket server time")?;
    let local_time_ms = chrono::Utc::now().timestamp_millis();
    let clock_skew_ms = local_time_ms.abs_diff(server_time_ms);
    anyhow::ensure!(
        clock_skew_ms <= 30_000,
        "local clock differs from venue by {clock_skew_ms}ms"
    );
    let geoblock: Value = client
        .get(&config.polymarket.geoblock_url)
        .send()
        .await
        .context("query geographic eligibility")?
        .error_for_status()
        .context("geographic eligibility endpoint rejected the request")?
        .json()
        .await
        .context("parse geographic eligibility")?;
    let blocked = geoblock
        .get("blocked")
        .and_then(Value::as_bool)
        .context("geographic eligibility response is missing boolean blocked")?;
    anyhow::ensure!(!blocked, "venue reports this egress location as blocked");
    Ok(json!({
        "protocol": version,
        "clock_skew_ms": clock_skew_ms,
        "geographic_eligibility": "allowed"
    }))
}

fn validate_mode_policy(config: &Config, policy: &RiskPolicy) -> Result<()> {
    if config.mode != Mode::Live {
        return Ok(());
    }
    anyhow::ensure!(
        !policy.allowed_condition_ids.is_empty() && !policy.allowed_token_ids.is_empty(),
        "live mode requires non-empty condition and token allowlists"
    );
    anyhow::ensure!(
        !policy.allowed_condition_ids.contains("*") && !policy.allowed_token_ids.contains("*"),
        "live mode refuses wildcard risk-policy allowlists"
    );
    anyhow::ensure!(
        !policy.version.to_ascii_lowercase().contains("example")
            && !policy.version.to_ascii_lowercase().contains("placeholder"),
        "live mode refuses placeholder risk-policy versions"
    );
    Ok(())
}

async fn control_request(api: &ApiArgs, path: &str, reason: &str) -> Result<()> {
    print_json(
        api_request(
            api,
            Method::POST,
            path,
            Some(&ReasonRequest {
                reason: reason.into(),
            }),
            true,
        )
        .await?,
    )
}

async fn api_request<T: Serialize + ?Sized>(
    api: &ApiArgs,
    method: Method,
    path: &str,
    body: Option<&T>,
    idempotent: bool,
) -> Result<Value> {
    let token = load_api_token(&api.token_file)?;
    let client = Client::builder().timeout(Duration::from_secs(45)).build()?;
    let mut request = client
        .request(method, format!("{}{}", api.url.trim_end_matches('/'), path))
        .bearer_auth(token.expose_secret());
    if idempotent {
        request = request.header(
            "Idempotency-Key",
            api.idempotency_key
                .clone()
                .unwrap_or_else(|| format!("cli-{}", Uuid::now_v7())),
        );
    }
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request.send().await?;
    let status = response.status();
    let body: Value = response.json().await?;
    anyhow::ensure!(status.is_success(), "API returned {status}: {body}");
    Ok(body)
}

fn capabilities() -> Capabilities {
    if cfg!(feature = "live") {
        Capabilities {
            schema_version: "oddsfox.capabilities.v1",
            modes: &["paper", "live"],
            signer: Some("local_file"),
        }
    } else {
        Capabilities {
            schema_version: "oddsfox.capabilities.v1",
            modes: &["paper"],
            signer: None,
        }
    }
}

fn load_api_token(path: &Path) -> Result<SecretString> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("read bearer token file {}", path.display()))?;
    let length =
        usize::try_from(file.metadata()?.len()).context("bearer token file is too large")?;
    let buffer_length = length
        .checked_add(1)
        .context("bearer token file is too large")?;
    let mut bytes = SecretBox::<Vec<u8>>::default();
    bytes.expose_secret_mut().resize(buffer_length, 0);
    file.read_exact(&mut bytes.expose_secret_mut()[..length])
        .context("read bearer token file")?;
    anyhow::ensure!(
        file.read(&mut bytes.expose_secret_mut()[length..])? == 0,
        "bearer token file changed while it was being read"
    );
    let contents = std::str::from_utf8(&bytes.expose_secret()[..length])
        .context("bearer token file must contain valid UTF-8")?;
    parse_api_token(contents)
}

fn parse_api_token(mut contents: &str) -> Result<SecretString> {
    if let Some(without_terminator) = contents.strip_suffix("\r\n") {
        contents = without_terminator;
    } else if let Some(without_terminator) = contents.strip_suffix('\n') {
        contents = without_terminator;
    }
    anyhow::ensure!(
        !contents.contains(['\r', '\n']),
        "bearer token file must contain exactly one line"
    );
    anyhow::ensure!(
        contents.trim() == contents,
        "bearer token file must not contain surrounding whitespace"
    );
    anyhow::ensure!(
        contents.chars().count() >= 32,
        "bearer token file must contain at least 32 characters"
    );
    Ok(SecretString::from(contents))
}

#[allow(clippy::needless_pass_by_value)]
fn print_json(value: Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer().json())
        .init();
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    error!(%error, "failed to wait for ctrl-c");
                }
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret as _;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn capabilities_contract_is_stable() {
        let encoded = serde_json::to_string(&capabilities()).unwrap();
        if cfg!(feature = "live") {
            assert_eq!(
                encoded,
                r#"{"schema_version":"oddsfox.capabilities.v1","modes":["paper","live"],"signer":"local_file"}"#
            );
        } else {
            assert_eq!(
                encoded,
                r#"{"schema_version":"oddsfox.capabilities.v1","modes":["paper"],"signer":null}"#
            );
        }
    }

    #[test]
    fn token_file_accepts_no_terminator_lf_or_crlf() {
        let token = "a".repeat(32);
        for suffix in ["", "\n", "\r\n"] {
            let candidate = format!("{token}{suffix}");
            let parsed = parse_api_token(&candidate).unwrap();
            assert_eq!(parsed.expose_secret(), &token);
        }
    }

    #[test]
    fn token_file_rejects_short_multiline_and_surrounding_whitespace() {
        for invalid in [
            "short".to_owned(),
            format!("{}\n{}\n", "a".repeat(32), "b".repeat(32)),
            format!(" {}", "a".repeat(32)),
            format!("{} ", "a".repeat(32)),
            format!("{}\n\n", "a".repeat(32)),
            format!("{}\r", "a".repeat(32)),
        ] {
            assert!(parse_api_token(&invalid).is_err());
        }
    }

    #[test]
    fn missing_token_file_is_rejected_without_exposing_a_token() {
        let path = tempdir().unwrap().path().join("missing-token");
        let error = load_api_token(&path).unwrap_err().to_string();
        assert!(error.contains("read bearer token file"));
    }
}
