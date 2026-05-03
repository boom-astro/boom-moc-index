//! GCN Kafka → MocIndex listener.
//!
//! Subscribes to live GCN topics on `gcn.nasa.gov`, decodes the embedded or
//! linked HEALPix skymap from each alert, thresholds it at the configured
//! credible level (default 0.9), and registers the resulting MOC into the
//! Valkey-backed [`MocIndex`].
//!
//! Mirrors the consumer loop in
//! `/Users/mcoughlin/Code/ORIGIN/origin/crates/mm-service/src/main.rs` and the
//! reqwest + exponential-backoff fetch in
//! `/Users/mcoughlin/Code/ORIGIN/origin/crates/mm-core/src/skymap_storage.rs`.
//!
//! Credentials are read from `.env` at the repo root (loaded via `dotenvy`).

#[allow(unused_imports)]
use boom_moc_index::moc::MocHasMaxDepth;
use boom_moc_index::{moc, MocIndex, MocMetadata, DEFAULT_INDEX_DEPTH};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::Utc;
use clap::Parser;
use rdkafka::{
    client::{ClientContext, OAuthToken},
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    ClientConfig, Message,
};
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;
use std::time::Duration;
use tracing::{error, info, warn};

/// Custom rdkafka context that supplies OAUTHBEARER tokens for GCN.
///
/// librdkafka's built-in OIDC token fetcher is broken on macOS — the SASL
/// handshake never completes, the consumer sits silently with zero traffic.
/// We bypass it and fetch the token ourselves via curl, exactly like ORIGIN's
/// `crates/mm-service/src/bin/gcn_correlator.rs` does.
struct GcnContext {
    client_id: String,
    client_secret: String,
}

impl ClientContext for GcnContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        info!("Fetching OAuth token from auth.gcn.nasa.gov...");
        let output = std::process::Command::new("curl")
            .args([
                "-s",
                "-X",
                "POST",
                "https://auth.gcn.nasa.gov/oauth2/token",
                "-H",
                "Content-Type: application/x-www-form-urlencoded",
                "-d",
                &format!(
                    "grant_type=client_credentials&client_id={}&client_secret={}&scope={}",
                    self.client_id, self.client_secret, "gcn.nasa.gov/kafka-public-consumer"
                ),
            ])
            .output()
            .map_err(|e| format!("Failed to run curl: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("curl failed: {stderr}").into());
        }

        let body = String::from_utf8(output.stdout)
            .map_err(|e| format!("Invalid UTF-8 from token endpoint: {e}"))?;

        #[derive(Deserialize)]
        struct TokenResponse {
            access_token: String,
            expires_in: u64,
        }

        let resp: TokenResponse = serde_json::from_str(&body)
            .map_err(|e| format!("Failed to parse token response: {e} (body: {body})"))?;

        let lifetime_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + (resp.expires_in as i64 * 1000);

        info!("OAuth token obtained (expires in {}s)", resp.expires_in);
        Ok(OAuthToken {
            token: resp.access_token,
            principal_name: "boom-moc-index".to_string(),
            lifetime_ms,
        })
    }
}

impl ConsumerContext for GcnContext {}

/// JSON topics that are accessible to public-consumer credentials. Mirrors
/// ORIGIN's `Config::development()` default list. LVK is quiet during O5
/// commissioning, but the Swift / Einstein Probe / IceCube topics fire often
/// enough to exercise the live path.
const DEFAULT_TOPICS: &[&str] = &[
    "igwn.gwalert",
    "gcn.notices.swift.bat.guano",
    "gcn.notices.einstein_probe.wxt.alert",
    "gcn.notices.icecube.lvk_nu_track_search",
    "gcn.notices.icecube.gold_bronze_track_alerts",
];

#[derive(Parser)]
#[command(name = "gcn-listener", about = "Stream GCN alerts → MocIndex")]
struct Args {
    /// Replay a saved alert payload from disk and exit (offline smoke test).
    #[arg(long)]
    replay_payload: Option<String>,

    /// Topic to attribute a replayed payload to (required with --replay-payload).
    #[arg(long)]
    replay_topic: Option<String>,

    /// HEALPix depth used by the meta-index.
    #[arg(long, default_value_t = DEFAULT_INDEX_DEPTH)]
    depth: u8,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // tracing-subscriber's "tracing-log" feature bridges `log` → `tracing`,
    // so librdkafka diagnostics (which use the `log` crate) surface here.
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "gcn_listener=info".to_string()),
        )
        .init();

    // Load .env from repo root (silently ignore if absent — env vars may be
    // exported some other way).
    let _ = dotenvy::dotenv();

    let args = Args::parse();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string());
    let credible_level: f64 = std::env::var("CREDIBLE_LEVEL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.9);
    let validity_seconds: u64 = std::env::var("VALIDITY_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(14 * 24 * 3600);

    let mut idx = MocIndex::open(&redis_url, args.depth).await?;
    info!(
        "Connected to Valkey at {} (depth={}, credible_level={}, validity={}s)",
        redis_url, args.depth, credible_level, validity_seconds
    );

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()?;

    // Offline-replay path: parse a saved payload, register, and exit.
    if let Some(path) = args.replay_payload.as_deref() {
        let topic = args
            .replay_topic
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--replay-payload requires --replay-topic"))?;
        let payload = std::fs::read_to_string(path)?;
        info!("Replaying payload from {} as topic {}", path, topic);
        match handle_alert(
            topic,
            &payload,
            &http,
            &mut idx,
            credible_level,
            validity_seconds,
        )
        .await
        {
            Ok(Some(moc_id)) => info!("Replay registered MOC: {}", moc_id),
            Ok(None) => warn!("Replay payload had no skymap to register"),
            Err(e) => error!("Replay failed: {}", e),
        }
        return Ok(());
    }

    // Live consumer.
    let client_id = std::env::var("GCN_CLIENT_ID")
        .map_err(|_| anyhow::anyhow!("GCN_CLIENT_ID not set (check .env)"))?;
    let client_secret = std::env::var("GCN_CLIENT_SECRET")
        .map_err(|_| anyhow::anyhow!("GCN_CLIENT_SECRET not set (check .env)"))?;
    let group_id =
        std::env::var("GCN_GROUP_ID").unwrap_or_else(|_| "boom-moc-index".to_string());
    // Default to earliest so a fresh consumer picks up the broker's recent
    // backlog (a few days of buffered alerts on each topic). Override with
    // GCN_OFFSET_RESET=latest for tail-only.
    let offset_reset =
        std::env::var("GCN_OFFSET_RESET").unwrap_or_else(|_| "earliest".to_string());

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", "kafka.gcn.nasa.gov");
    config.set("security.protocol", "sasl_ssl");
    config.set("sasl.mechanisms", "OAUTHBEARER");
    config.set("group.id", &group_id);
    config.set("session.timeout.ms", "45000");
    config.set("enable.auto.commit", "false");
    config.set("auto.offset.reset", &offset_reset);
    if let Ok(debug) = std::env::var("GCN_KAFKA_DEBUG") {
        config.set("debug", &debug);
    }

    let context = GcnContext {
        client_id: client_id.clone(),
        client_secret: client_secret.clone(),
    };
    let consumer: StreamConsumer<GcnContext> = config.create_with_context(context)?;
    let mut topics: Vec<&str> = DEFAULT_TOPICS.to_vec();
    if std::env::var("GCN_LOG_HEARTBEAT").is_ok() {
        topics.push("gcn.heartbeat");
    }
    consumer.subscribe(&topics)?;
    info!(
        "Subscribed to {} topics (group_id={}, offset_reset={}): {}",
        topics.len(),
        group_id,
        offset_reset,
        topics.join(", ")
    );
    info!("Waiting for alerts...");

    loop {
        match consumer.recv().await {
            Err(err) => {
                // Back off briefly so transient errors (e.g. a single
                // TopicAuthorizationFailed during partition assignment) don't
                // spin the loop and flood the log.
                error!("Kafka receive error: {}", err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(msg) => {
                let topic = msg.topic().to_string();
                if topic == "gcn.heartbeat" {
                    if std::env::var("GCN_LOG_HEARTBEAT").is_ok() {
                        info!("heartbeat");
                    }
                    continue;
                }
                let Some(payload_res) = msg.payload_view::<str>() else {
                    continue;
                };
                let payload = match payload_res {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        error!("Failed to decode payload from {}: {}", topic, e);
                        continue;
                    }
                };
                info!("Received alert from {} ({} bytes)", topic, payload.len());
                match handle_alert(
                    &topic,
                    &payload,
                    &http,
                    &mut idx,
                    credible_level,
                    validity_seconds,
                )
                .await
                {
                    Ok(Some(moc_id)) => info!("Registered MOC: {}", moc_id),
                    Ok(None) => info!("No skymap in alert from {} — skipped", topic),
                    Err(e) => error!("Failed to process alert from {}: {}", topic, e),
                }
            }
        }
    }
}

/// Process a single alert payload: extract the skymap, build a MOC, register.
/// Returns the assigned `moc_id` on success, `None` if no skymap was present.
async fn handle_alert(
    topic: &str,
    payload: &str,
    http: &reqwest::Client,
    idx: &mut MocIndex,
    credible_level: f64,
    validity_seconds: u64,
) -> anyhow::Result<Option<String>> {
    let json: Value = serde_json::from_str(payload)?;

    // Skip retractions outright (no skymap, nothing to do).
    if json.get("alert_type").and_then(|v| v.as_str()) == Some("RETRACTION") {
        return Ok(None);
    }

    let Some(skymap_bytes) = extract_skymap(topic, &json, http).await? else {
        return Ok(None);
    };

    let hpx_moc = moc::moc_from_skymap_bytes(&skymap_bytes, credible_level)?;
    let fits_bytes = moc::moc_to_fits_bytes(&hpx_moc)?;

    let moc_id = derive_moc_id(topic, &json);
    let trigger_time = derive_trigger_time(&json);
    let source = derive_source(topic);

    let metadata = MocMetadata {
        source,
        trigger_time,
        credible_level,
        validity_seconds,
        coverage_fraction: hpx_moc.coverage_percentage(),
        native_depth: hpx_moc.depth_max(),
    };

    let n_cells = idx
        .register(&moc_id, &hpx_moc, &fits_bytes, &metadata)
        .await?;
    info!(
        "  → {} cells indexed (coverage={:.4}%, native_depth={})",
        n_cells,
        metadata.coverage_fraction * 100.0,
        metadata.native_depth
    );
    Ok(Some(moc_id))
}

/// Pull skymap FITS bytes out of a parsed alert JSON. Three sources, in order:
///   1. `event.skymap` base64 (igwn.gwalert)
///   2. `healpix_file` base64 (some non-LVK JSON topics)
///   3. URL field (`skymap_url`, `urls.skymap_fits`, `url`) — fetched over HTTP
async fn extract_skymap(
    topic: &str,
    json: &Value,
    http: &reqwest::Client,
) -> anyhow::Result<Option<Vec<u8>>> {
    // 1. igwn.gwalert: base64 multi-order FITS at event.skymap
    if topic == "igwn.gwalert" {
        if let Some(b64) = json.pointer("/event/skymap").and_then(|v| v.as_str()) {
            let bytes = BASE64
                .decode(b64.as_bytes())
                .map_err(|e| anyhow::anyhow!("base64 decode of event.skymap failed: {}", e))?;
            return Ok(Some(bytes));
        }
        return Ok(None);
    }

    // 2. healpix_file base64
    if let Some(b64) = json.get("healpix_file").and_then(|v| v.as_str()) {
        let bytes = BASE64
            .decode(b64.as_bytes())
            .map_err(|e| anyhow::anyhow!("base64 decode of healpix_file failed: {}", e))?;
        return Ok(Some(bytes));
    }

    // 3. URL — try a few common keys
    let url = json
        .get("skymap_url")
        .and_then(|v| v.as_str())
        .or_else(|| json.pointer("/urls/skymap_fits").and_then(|v| v.as_str()))
        .or_else(|| json.get("url").and_then(|v| v.as_str()));

    if let Some(url) = url {
        let bytes = fetch_with_retry(http, url).await?;
        return Ok(Some(bytes));
    }

    Ok(None)
}

/// HTTP GET with up to 3 attempts and 2^(attempt-1) second backoff.
/// Mirrors `crates/mm-core/src/skymap_storage.rs` from ORIGIN.
async fn fetch_with_retry(http: &reqwest::Client, url: &str) -> anyhow::Result<Vec<u8>> {
    const MAX_RETRIES: u32 = 3;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_RETRIES {
        match http.get(url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let bytes = resp.bytes().await?;
                return Ok(bytes.to_vec());
            }
            Ok(resp) => {
                last_err = Some(anyhow::anyhow!("HTTP {}", resp.status()));
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!("HTTP error: {}", e));
            }
        }
        if attempt < MAX_RETRIES {
            let wait = 2u64.pow(attempt - 1);
            warn!("fetch {} attempt {}/{} failed; retrying in {}s", url, attempt, MAX_RETRIES, wait);
            tokio::time::sleep(Duration::from_secs(wait)).await;
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("fetch failed")))
}

fn derive_moc_id(topic: &str, json: &Value) -> String {
    // Prefer source-specific identifiers; fall back to topic + receive time.
    if let Some(s) = json.get("superevent_id").and_then(|v| v.as_str()) {
        let alert_type = json
            .get("alert_type")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");
        return format!("LVK-{}-{}", s, alert_type);
    }
    for key in ["trigger_id", "trigger_name", "id", "event_id"] {
        if let Some(v) = json.get(key).and_then(|v| v.as_str()) {
            return format!("{}-{}", short_topic(topic), v);
        }
    }
    format!("{}-{}", short_topic(topic), Utc::now().format("%Y%m%dT%H%M%SZ"))
}

fn derive_trigger_time(json: &Value) -> String {
    for key in ["trigger_time", "time", "reference_time", "time_created"] {
        if let Some(v) = json.get(key).and_then(|v| v.as_str()) {
            return v.to_string();
        }
    }
    if let Some(v) = json.pointer("/event/time").and_then(|v| v.as_str()) {
        return v.to_string();
    }
    Utc::now().to_rfc3339()
}

fn derive_source(topic: &str) -> String {
    if topic == "igwn.gwalert" {
        return "LVK".to_string();
    }
    if topic.contains("swift.bat") {
        return "Swift-BAT".to_string();
    }
    if topic.contains("einstein_probe") {
        return "EinsteinProbe-WXT".to_string();
    }
    if topic.contains("icecube") {
        return "IceCube".to_string();
    }
    if topic.contains("fermi") {
        return "Fermi".to_string();
    }
    topic.to_string()
}

fn short_topic(topic: &str) -> &str {
    // e.g. "gcn.notices.swift.bat.guano" → "swift.bat.guano"
    topic
        .strip_prefix("gcn.notices.")
        .unwrap_or_else(|| topic.strip_prefix("gcn.").unwrap_or(topic))
}
