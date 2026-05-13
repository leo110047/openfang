//! Durable OpenFang ops event pipeline.
//!
//! Runtime warnings must not depend on an agent deciding to report them, and
//! observability must not block cron, heartbeat, or tool execution paths. Calls
//! to `record_system_event` only enqueue a bounded local outbox item; a
//! detached worker persists the event to OpenFang's local ops event store and
//! sends high-severity alerts to Discord with backoff.

use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openfang_memory::migration::run_migrations;
use openfang_types::config::{load_config, openfang_home, KernelConfig};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const FLUSH_BACKOFF: Duration = Duration::from_secs(30);
const MAX_RECORD_RETRY_BACKOFF: Duration = Duration::from_secs(15 * 60);
const MAX_OUTBOX_RECORDS: usize = 1_000;
const FLUSH_BATCH_SIZE: usize = 50;
const MAX_FIELD_CHARS: usize = 8_000;
const MAX_PAYLOAD_STRING_CHARS: usize = 4_000;
const DISCORD_ALERT_SUPPRESSION: Duration = Duration::from_secs(6 * 60 * 60);
const DISCORD_ALERT_MESSAGE_CHARS: usize = 1_900;
const DISCORD_ALERT_REASON_CHARS: usize = 520;
const DISCORD_ALERT_IMPACT_CHARS: usize = 360;
const DISCORD_ALERT_AGENT_CHARS: usize = 120;
const DISCORD_ALERT_JOB_CHARS: usize = 120;

pub const EVENT_TYPE_MEMORY_EMBEDDING_FAILED: &str = "memory_embedding_failed";
pub const EVENT_TYPE_LEGACY_EMBEDDING_FAILED: &str = "embedding_failed";
pub const EVENT_TYPE_CRON_DELIVERY_FAILED: &str = "cron_delivery_failed";
pub const EVENT_TYPE_CRON_STATE_PERSIST_FAILED: &str = "cron_state_persist_failed";

static OUTBOX_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));
static FLUSH_RUNNING: AtomicBool = AtomicBool::new(false);
static DISCORD_ALERT_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));
static DISCORD_ALERT_FLUSH_RUNNING: AtomicBool = AtomicBool::new(false);
static OPS_EVENT_SENDER: OnceLock<tokio::sync::mpsc::UnboundedSender<OpenFangOpsEvent>> =
    OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenFangOpsEvent {
    pub severity: String,
    pub source: String,
    pub component: String,
    pub event_type: String,
    pub title: String,
    pub message: String,
    pub technical_detail: String,
    pub impact: String,
    pub dedupe_key: String,
    pub agent: String,
    pub run_id: String,
    pub job_id: String,
    pub payload_json: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboxRecord {
    id: String,
    event: OpenFangOpsEvent,
    attempts: u32,
    first_queued_at_ms: u64,
    last_attempt_at_ms: Option<u64>,
    last_error: String,
}

#[derive(Debug)]
enum DeliveryOutcome {
    Delivered,
    Retry(String),
    Drop(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscordAlertState {
    sent_at_by_key: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
struct DiscordAlertConfig {
    token: String,
    channel_id: String,
}

impl OpenFangOpsEvent {
    pub fn warning(
        component: &'static str,
        event_type: &'static str,
        title: impl Into<String>,
    ) -> Self {
        Self {
            severity: "warning".to_string(),
            source: "openfang".to_string(),
            component: component.to_string(),
            event_type: event_type.to_string(),
            title: title.into(),
            message: String::new(),
            technical_detail: String::new(),
            impact: String::new(),
            dedupe_key: String::new(),
            agent: String::new(),
            run_id: String::new(),
            job_id: String::new(),
            payload_json: serde_json::json!({}),
        }
    }

    pub fn error(
        component: &'static str,
        event_type: &'static str,
        title: impl Into<String>,
    ) -> Self {
        let mut event = Self::warning(component, event_type, title);
        event.severity = "error".to_string();
        event
    }

    pub fn with_agent(mut self, agent: impl Into<String>) -> Self {
        self.agent = agent.into();
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    pub fn with_detail(mut self, technical_detail: impl Into<String>) -> Self {
        self.technical_detail = technical_detail.into();
        self
    }

    pub fn with_impact(mut self, impact: impl Into<String>) -> Self {
        self.impact = impact.into();
        self
    }

    pub fn with_dedupe_key(mut self, dedupe_key: impl Into<String>) -> Self {
        self.dedupe_key = dedupe_key.into();
        self
    }

    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload_json = payload;
        self
    }
}

pub async fn record_system_event(event: OpenFangOpsEvent) {
    let event = sanitize_event(event);
    if let Err(err) = ops_event_sender().send(event) {
        warn!("OpenFang ops event enqueue failed: {err}");
    }
}

fn ops_event_sender() -> &'static tokio::sync::mpsc::UnboundedSender<OpenFangOpsEvent> {
    OPS_EVENT_SENDER.get_or_init(|| {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(ops_event_enqueue_worker(receiver));
        sender
    })
}

async fn ops_event_enqueue_worker(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<OpenFangOpsEvent>,
) {
    while let Some(event) = receiver.recv().await {
        enqueue_ops_event(event).await;
    }
}

async fn enqueue_ops_event(event: OpenFangOpsEvent) {
    let should_notify_discord = should_notify_discord_alert(&event);
    let path = outbox_path();
    {
        let _guard = OUTBOX_LOCK.lock().await;
        if let Err(err) = append_outbox_record(&path, OutboxRecord::new(event.clone())) {
            warn!(path = ?path, "OpenFang ops event outbox append failed: {err}");
            return;
        }
    }
    schedule_outbox_flush();
    if should_notify_discord {
        enqueue_discord_alert(event).await;
    }
}

fn schedule_outbox_flush() {
    if FLUSH_RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    tokio::spawn(async {
        flush_outbox_worker().await;
        FLUSH_RUNNING.store(false, Ordering::Release);
        if outbox_has_records().await {
            schedule_outbox_flush();
        }
    });
}

async fn flush_outbox_worker() {
    loop {
        match flush_outbox_batch().await {
            Ok(stats) if stats.remaining == 0 => break,
            Ok(stats) if stats.ready_count == 0 && stats.retry_count > 0 => {
                tokio::time::sleep(stats.next_ready_in.unwrap_or(FLUSH_BACKOFF)).await;
            }
            Ok(stats) if stats.ready_count == 0 => {
                tokio::time::sleep(stats.next_ready_in.unwrap_or(FLUSH_BACKOFF)).await;
            }
            Ok(_) => {}
            Err(err) => {
                warn!("OpenFang ops event outbox flush failed: {err}");
                tokio::time::sleep(FLUSH_BACKOFF).await;
            }
        }
    }
}

#[derive(Debug, Default)]
struct FlushStats {
    remaining: usize,
    retry_count: usize,
    ready_count: usize,
    next_ready_in: Option<Duration>,
}

async fn flush_outbox_batch() -> Result<FlushStats, String> {
    let path = outbox_path();
    let batch = {
        let _guard = OUTBOX_LOCK.lock().await;
        let records = load_outbox_records(&path)?;
        let now = now_ms();
        let batch = select_outbox_batch(&records, now);
        if batch.is_empty() {
            return Ok(flush_stats_for_records(&records, now));
        }
        batch
    };

    let mut outcomes = HashMap::new();
    for record in batch {
        let outcome = persist_ops_event(&record.event);
        outcomes.insert(record.id, outcome);
    }

    let _guard = OUTBOX_LOCK.lock().await;
    let mut records = load_outbox_records(&path)?;
    apply_delivery_outcomes(&mut records, outcomes);
    enforce_outbox_limit(&mut records);
    let stats = flush_stats_for_records(&records, now_ms());
    write_outbox_records(&path, &records)?;
    Ok(stats)
}

async fn outbox_has_records() -> bool {
    let path = outbox_path();
    let _guard = OUTBOX_LOCK.lock().await;
    match load_outbox_records(&path) {
        Ok(records) => !records.is_empty(),
        Err(err) => {
            warn!(path = ?path, "OpenFang ops event outbox check failed: {err}");
            false
        }
    }
}

async fn enqueue_discord_alert(event: OpenFangOpsEvent) {
    let path = discord_alert_outbox_path();
    {
        let _guard = DISCORD_ALERT_LOCK.lock().await;
        if let Err(err) = append_discord_alert_record(&path, OutboxRecord::new(event)) {
            warn!(path = ?path, "Discord system alert outbox append failed: {err}");
            return;
        }
    }
    schedule_discord_alert_flush();
}

fn schedule_discord_alert_flush() {
    if DISCORD_ALERT_FLUSH_RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    tokio::spawn(async {
        flush_discord_alert_worker().await;
        DISCORD_ALERT_FLUSH_RUNNING.store(false, Ordering::Release);
        if discord_alert_outbox_has_records().await {
            schedule_discord_alert_flush();
        }
    });
}

async fn flush_discord_alert_worker() {
    loop {
        match flush_discord_alert_batch().await {
            Ok(stats) if stats.remaining == 0 => break,
            Ok(stats) if stats.ready_count == 0 => {
                tokio::time::sleep(stats.next_ready_in.unwrap_or(FLUSH_BACKOFF)).await;
            }
            Ok(_) => {}
            Err(err) => {
                warn!("Discord system alert outbox flush failed: {err}");
                tokio::time::sleep(FLUSH_BACKOFF).await;
            }
        }
    }
}

async fn flush_discord_alert_batch() -> Result<FlushStats, String> {
    let config = discord_alert_config()?;
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|err| format!("Discord alert client build failed: {err}"))?;
    let path = discord_alert_outbox_path();
    let batch = {
        let _guard = DISCORD_ALERT_LOCK.lock().await;
        let records = load_outbox_records(&path)?;
        let now = now_ms();
        let batch = select_outbox_batch(&records, now);
        if batch.is_empty() {
            return Ok(flush_stats_for_records(&records, now));
        }
        batch
    };

    let mut outcomes = HashMap::new();
    for record in batch {
        let message = format_discord_system_event_alert(&record.event);
        let outcome = post_discord_alert(&client, &config, &message).await;
        outcomes.insert(record.id, outcome);
    }

    let _guard = DISCORD_ALERT_LOCK.lock().await;
    let mut records = load_outbox_records(&path)?;
    let mut state = load_discord_alert_state(&discord_alert_state_path())?;
    apply_discord_alert_delivery_outcomes(&mut records, outcomes, &mut state);
    enforce_outbox_limit(&mut records);
    prune_discord_alert_state(&mut state);
    let stats = flush_stats_for_records(&records, now_ms());
    write_outbox_records(&path, &records)?;
    write_discord_alert_state(&discord_alert_state_path(), &state)?;
    Ok(stats)
}

async fn discord_alert_outbox_has_records() -> bool {
    let path = discord_alert_outbox_path();
    let _guard = DISCORD_ALERT_LOCK.lock().await;
    match load_outbox_records(&path) {
        Ok(records) => !records.is_empty(),
        Err(err) => {
            warn!(path = ?path, "Discord system alert outbox check failed: {err}");
            false
        }
    }
}

fn select_outbox_batch(records: &[OutboxRecord], now_ms: u64) -> Vec<OutboxRecord> {
    let mut ready = records
        .iter()
        .filter(|record| record_ready_at_ms(record, now_ms))
        .cloned()
        .collect::<Vec<_>>();
    ready.sort_by_key(|record| {
        (
            record.last_attempt_at_ms.unwrap_or(0),
            record.first_queued_at_ms,
        )
    });
    ready.into_iter().take(FLUSH_BATCH_SIZE).collect()
}

fn flush_stats_for_records(records: &[OutboxRecord], now_ms: u64) -> FlushStats {
    let retry_count = records
        .iter()
        .filter(|record| record.last_attempt_at_ms.is_some())
        .count();
    let ready_count = records
        .iter()
        .filter(|record| record_ready_at_ms(record, now_ms))
        .count();
    let next_ready_in = records
        .iter()
        .filter_map(|record| record_next_ready_delay(record, now_ms))
        .min();
    FlushStats {
        remaining: records.len(),
        retry_count,
        ready_count,
        next_ready_in,
    }
}

fn record_ready_at_ms(record: &OutboxRecord, now_ms: u64) -> bool {
    record_next_ready_delay(record, now_ms).is_none()
}

fn record_next_ready_delay(record: &OutboxRecord, now_ms: u64) -> Option<Duration> {
    let last_attempt_at_ms = record.last_attempt_at_ms?;
    let ready_at = last_attempt_at_ms.saturating_add(duration_millis(record_retry_backoff(record)));
    if ready_at <= now_ms {
        None
    } else {
        Some(Duration::from_millis(ready_at - now_ms))
    }
}

fn record_retry_backoff(record: &OutboxRecord) -> Duration {
    let exponent = record.attempts.saturating_sub(1).min(5);
    let multiplier = 1_u64 << exponent;
    FLUSH_BACKOFF
        .saturating_mul(multiplier as u32)
        .min(MAX_RECORD_RETRY_BACKOFF)
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

async fn post_discord_alert(
    client: &reqwest::Client,
    config: &DiscordAlertConfig,
    message: &str,
) -> DeliveryOutcome {
    let url = format!("{DISCORD_API_BASE}/channels/{}/messages", config.channel_id);
    let response = match client
        .post(url)
        .bearer_auth(&config.token)
        .json(&serde_json::json!({ "content": message }))
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => return DeliveryOutcome::Retry(err.to_string()),
    };
    let status = response.status();
    if status.is_success() {
        return DeliveryOutcome::Delivered;
    }
    let text = response.text().await.unwrap_or_default();
    let detail = format!(
        "Discord returned HTTP {status}: {}",
        truncate_chars(&text, 1_000)
    );
    if status.is_client_error() {
        DeliveryOutcome::Drop(detail)
    } else {
        DeliveryOutcome::Retry(detail)
    }
}

fn apply_delivery_outcomes(
    records: &mut Vec<OutboxRecord>,
    outcomes: HashMap<String, DeliveryOutcome>,
) {
    let now = now_ms();
    let mut delivered_or_dropped = HashSet::new();
    for (id, outcome) in outcomes {
        match outcome {
            DeliveryOutcome::Delivered => {
                delivered_or_dropped.insert(id);
            }
            DeliveryOutcome::Drop(reason) => {
                warn!(record_id = %id, reason = %reason, "Dropping rejected OpenFang ops event");
                delivered_or_dropped.insert(id);
            }
            DeliveryOutcome::Retry(reason) => {
                if let Some(record) = records.iter_mut().find(|record| record.id == id) {
                    record.attempts = record.attempts.saturating_add(1);
                    record.last_attempt_at_ms = Some(now);
                    record.last_error = truncate_chars(&reason, 1_000);
                }
            }
        }
    }
    records.retain(|record| !delivered_or_dropped.contains(&record.id));
}

fn apply_discord_alert_delivery_outcomes(
    records: &mut Vec<OutboxRecord>,
    outcomes: HashMap<String, DeliveryOutcome>,
    state: &mut DiscordAlertState,
) {
    let now = now_ms();
    let mut delivered_or_dropped = HashSet::new();
    for (id, outcome) in outcomes {
        match outcome {
            DeliveryOutcome::Delivered => {
                if let Some(record) = records.iter().find(|record| record.id == id) {
                    if let Some(key) = discord_alert_key(&record.event) {
                        state.sent_at_by_key.insert(key, now);
                    }
                    if let Err(err) = mark_ops_event_notified(&record.event) {
                        warn!(
                            record_id = %id,
                            "Failed to update OpenFang ops event notification timestamp: {err}"
                        );
                    }
                }
                delivered_or_dropped.insert(id);
            }
            DeliveryOutcome::Drop(reason) => {
                warn!(record_id = %id, reason = %reason, "Dropping rejected Discord system alert");
                delivered_or_dropped.insert(id);
            }
            DeliveryOutcome::Retry(reason) => {
                if let Some(record) = records.iter_mut().find(|record| record.id == id) {
                    record.attempts = record.attempts.saturating_add(1);
                    record.last_attempt_at_ms = Some(now);
                    record.last_error = truncate_chars(&reason, 1_000);
                }
            }
        }
    }
    records.retain(|record| !delivered_or_dropped.contains(&record.id));
}

fn discord_alert_config() -> Result<DiscordAlertConfig, String> {
    let kernel_config = load_openfang_config();
    let discord = kernel_config
        .channels
        .discord
        .ok_or("OpenFang Discord channel config is not enabled")?;

    let channel_id = std::env::var("OPENFANG_SYSTEM_EVENT_DISCORD_CHANNEL_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| discord.system_event_channel_id.clone())
        .or_else(|| discord.default_channel_id.clone())
        .ok_or("Discord system_event_channel_id or default_channel_id is required")?;
    validate_discord_channel_id(&channel_id)?;
    if !discord.allowed_channels.is_empty() && !discord.allowed_channels.contains(&channel_id) {
        return Err(format!(
            "Discord system event channel {channel_id} is not listed in allowed_channels"
        ));
    }

    let token = read_openfang_secret(&discord.bot_token_env)
        .map_err(|err| {
            format!(
                "Discord bot token {} is not available: {err}",
                discord.bot_token_env
            )
        })?
        .trim()
        .to_string();
    if token.is_empty() {
        return Err(format!(
            "Discord bot token env {} is empty",
            discord.bot_token_env
        ));
    }

    Ok(DiscordAlertConfig { token, channel_id })
}

fn read_openfang_secret(key: &str) -> Result<String, String> {
    if key.trim().is_empty() {
        return Err("secret key name is empty".to_string());
    }
    if let Ok(value) = std::env::var(key) {
        let value = value.trim().to_string();
        if !value.is_empty() {
            return Ok(value);
        }
    }
    for path in [
        openfang_home_dir().join(".env"),
        openfang_home_dir().join("secrets.env"),
    ] {
        if let Some(value) = read_secret_from_file(&path, key)? {
            return Ok(value);
        }
    }
    Err("not found in process env, ~/.openfang/.env, or ~/.openfang/secrets.env".to_string())
}

fn read_secret_from_file(path: &Path, key: &str) -> Result<Option<String>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let content =
        fs::read_to_string(path).map_err(|err| format!("unable to read {path:?}: {err}"))?;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        if name.trim() != key {
            continue;
        }
        let value = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        if !value.is_empty() {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn persist_ops_event(event: &OpenFangOpsEvent) -> DeliveryOutcome {
    match open_ops_db_connection().and_then(|conn| upsert_ops_event(&conn, event)) {
        Ok(()) => DeliveryOutcome::Delivered,
        Err(err) => DeliveryOutcome::Retry(err),
    }
}

fn mark_ops_event_notified(event: &OpenFangOpsEvent) -> Result<(), String> {
    let dedupe_key = event.dedupe_key.trim();
    if dedupe_key.is_empty() {
        return Ok(());
    }
    let conn = open_ops_db_connection()?;
    mark_ops_event_notified_in_connection(&conn, dedupe_key, &now_iso())
}

fn mark_ops_event_notified_in_connection(
    conn: &Connection,
    dedupe_key: &str,
    notified_at: &str,
) -> Result<(), String> {
    conn.execute(
        "UPDATE ops_events
         SET last_notified_at = ?1
         WHERE dedupe_key = ?2 AND status IN ('open', 'acknowledged')",
        params![notified_at, dedupe_key],
    )
    .map(|_| ())
    .map_err(|err| format!("update ops event notification timestamp failed: {err}"))
}

fn open_ops_db_connection() -> Result<Connection, String> {
    let db_path = ops_db_path()?;
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let conn = Connection::open(&db_path)
        .map_err(|err| format!("unable to open OpenFang ops DB {db_path:?}: {err}"))?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
        .map_err(|err| format!("unable to configure OpenFang ops DB {db_path:?}: {err}"))?;
    run_migrations(&conn).map_err(|err| format!("unable to migrate OpenFang ops DB: {err}"))?;
    Ok(conn)
}

fn ops_db_path() -> Result<PathBuf, String> {
    let config = load_openfang_config();
    Ok(config
        .memory
        .sqlite_path
        .unwrap_or_else(|| config.data_dir.join("openfang.db")))
}

fn load_openfang_config() -> KernelConfig {
    load_config(None)
}

fn upsert_ops_event(conn: &Connection, event: &OpenFangOpsEvent) -> Result<(), String> {
    let payload_json = serde_json::to_string(&event.payload_json).map_err(|err| err.to_string())?;
    let now = now_iso();
    conn.execute("BEGIN IMMEDIATE", [])
        .map_err(|err| format!("begin ops event transaction failed: {err}"))?;
    let result = upsert_ops_event_in_transaction(conn, event, &payload_json, &now);
    match result {
        Ok(()) => conn
            .execute("COMMIT", [])
            .map(|_| ())
            .map_err(|err| format!("commit ops event transaction failed: {err}")),
        Err(err) => {
            let _ = conn.execute("ROLLBACK", []);
            Err(err)
        }
    }
}

fn upsert_ops_event_in_transaction(
    conn: &Connection,
    event: &OpenFangOpsEvent,
    payload_json: &str,
    now: &str,
) -> Result<(), String> {
    let existing = if event.dedupe_key.trim().is_empty() {
        None
    } else {
        conn.query_row(
            "SELECT id, severity FROM ops_events
             WHERE dedupe_key = ?1 AND status IN ('open', 'acknowledged')
             ORDER BY last_seen_at DESC
             LIMIT 1",
            params![event.dedupe_key],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|err| format!("lookup ops event dedupe failed: {err}"))?
    };

    if let Some((id, existing_severity)) = existing {
        let retained_severity = max_severity(&existing_severity, &event.severity);
        conn.execute(
            "UPDATE ops_events
             SET severity = ?1,
                 source = ?2,
                 component = ?3,
                 event_type = ?4,
                 title = ?5,
                 message = ?6,
                 technical_detail = ?7,
                 impact = ?8,
                 agent = ?9,
                 run_id = ?10,
                 job_id = ?11,
                 occurrences = occurrences + 1,
                 last_seen_at = ?12,
                 payload_json = ?13
             WHERE id = ?14",
            params![
                retained_severity,
                event.source,
                event.component,
                event.event_type,
                event.title,
                event.message,
                event.technical_detail,
                event.impact,
                event.agent,
                event.run_id,
                event.job_id,
                now,
                payload_json,
                id
            ],
        )
        .map_err(|err| format!("update ops event failed: {err}"))?;
        return Ok(());
    }

    conn.execute(
        "INSERT INTO ops_events (
            id, severity, source, component, event_type, title, message,
            technical_detail, impact, dedupe_key, agent, run_id, job_id,
            status, occurrences, first_seen_at, last_seen_at, last_notified_at,
            payload_json
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
            'open', 1, ?14, ?14, NULL, ?15
        )",
        params![
            format!("ops-event-{}", Uuid::new_v4()),
            event.severity,
            event.source,
            event.component,
            event.event_type,
            event.title,
            event.message,
            event.technical_detail,
            event.impact,
            event.dedupe_key,
            event.agent,
            event.run_id,
            event.job_id,
            now,
            payload_json
        ],
    )
    .map_err(|err| format!("insert ops event failed: {err}"))?;
    Ok(())
}

fn max_severity(left: &str, right: &str) -> String {
    if severity_rank(left) <= severity_rank(right) {
        normalize_severity(left)
    } else {
        normalize_severity(right)
    }
}

fn severity_rank(value: &str) -> u8 {
    match normalize_severity(value).as_str() {
        "critical" => 1,
        "error" => 2,
        "warning" => 3,
        "info" => 4,
        _ => 4,
    }
}

fn normalize_severity(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "critical" => "critical".to_string(),
        "error" => "error".to_string(),
        "warning" => "warning".to_string(),
        "info" => "info".to_string(),
        _ => "info".to_string(),
    }
}

fn outbox_path() -> PathBuf {
    openfang_home_dir().join("ops_events_outbox.jsonl")
}

fn discord_alert_outbox_path() -> PathBuf {
    openfang_home_dir().join("system_event_discord_alerts_outbox.jsonl")
}

fn discord_alert_state_path() -> PathBuf {
    openfang_home_dir().join("system_event_discord_alerts_state.json")
}

fn openfang_home_dir() -> PathBuf {
    openfang_home()
}

fn should_notify_discord_alert(event: &OpenFangOpsEvent) -> bool {
    matches!(event.severity.as_str(), "error" | "critical")
        || is_actionable_warning_for_discord(event)
}

fn is_actionable_warning_for_discord(event: &OpenFangOpsEvent) -> bool {
    if event.severity != "warning" {
        return false;
    }

    matches!(
        (event.component.as_str(), event.event_type.as_str()),
        ("cron", EVENT_TYPE_CRON_DELIVERY_FAILED)
            | ("cron", EVENT_TYPE_CRON_STATE_PERSIST_FAILED)
            | ("memory", EVENT_TYPE_MEMORY_EMBEDDING_FAILED)
            | ("memory", EVENT_TYPE_LEGACY_EMBEDDING_FAILED)
    )
}

fn append_discord_alert_record(path: &Path, record: OutboxRecord) -> Result<(), String> {
    append_discord_alert_record_with_state(path, &discord_alert_state_path(), record)
}

fn append_discord_alert_record_with_state(
    path: &Path,
    state_path: &Path,
    record: OutboxRecord,
) -> Result<(), String> {
    if let Some(key) = discord_alert_key(&record.event) {
        let state = load_discord_alert_state(state_path)?;
        if let Some(last_sent_at_ms) = state.sent_at_by_key.get(&key) {
            let suppress_until =
                last_sent_at_ms.saturating_add(duration_millis(DISCORD_ALERT_SUPPRESSION));
            if suppress_until > now_ms() {
                return Ok(());
            }
        }
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let mut records = match load_outbox_records(path) {
        Ok(records) => records,
        Err(err) => {
            quarantine_outbox(path)?;
            warn!(path = ?path, "Quarantined invalid Discord alert outbox before appending: {err}");
            Vec::new()
        }
    };
    if let Some(key) = discord_alert_key(&record.event) {
        if records
            .iter()
            .any(|existing| discord_alert_key(&existing.event).as_deref() == Some(key.as_str()))
        {
            return Ok(());
        }
    }
    records.push(record);
    enforce_outbox_limit(&mut records);
    write_outbox_records(path, &records)
}

fn discord_alert_key(event: &OpenFangOpsEvent) -> Option<String> {
    let key = event.dedupe_key.trim();
    if key.is_empty() {
        None
    } else {
        Some(key.to_string())
    }
}

fn load_discord_alert_state(path: &Path) -> Result<DiscordAlertState, String> {
    if !path.exists() {
        return Ok(DiscordAlertState {
            sent_at_by_key: HashMap::new(),
        });
    }
    let content = fs::read_to_string(path).map_err(|err| err.to_string())?;
    serde_json::from_str(&content).map_err(|err| format!("invalid Discord alert state JSON: {err}"))
}

fn write_discord_alert_state(path: &Path, state: &DiscordAlertState) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let content = serde_json::to_string_pretty(state).map_err(|err| err.to_string())?;
    fs::write(&tmp_path, content).map_err(|err| err.to_string())?;
    fs::rename(tmp_path, path).map_err(|err| err.to_string())
}

fn prune_discord_alert_state(state: &mut DiscordAlertState) {
    let cutoff = now_ms().saturating_sub(duration_millis(DISCORD_ALERT_SUPPRESSION) * 4);
    state
        .sent_at_by_key
        .retain(|_, sent_at_ms| *sent_at_ms >= cutoff);
}

impl OutboxRecord {
    fn new(event: OpenFangOpsEvent) -> Self {
        Self {
            id: format!("outbox-record-{}", Uuid::new_v4()),
            event,
            attempts: 0,
            first_queued_at_ms: now_ms(),
            last_attempt_at_ms: None,
            last_error: String::new(),
        }
    }
}

fn append_outbox_record(path: &Path, record: OutboxRecord) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let mut records = match load_outbox_records(path) {
        Ok(records) => records,
        Err(err) => {
            quarantine_outbox(path)?;
            warn!(path = ?path, "Quarantined invalid OpenFang ops event outbox before appending: {err}");
            Vec::new()
        }
    };
    records.push(record);
    enforce_outbox_limit(&mut records);
    write_outbox_records(path, &records)
}

fn load_outbox_records(path: &Path) -> Result<Vec<OutboxRecord>, String> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(path).map_err(|err| err.to_string())?;
    let mut records = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<OutboxRecord>(line) {
            Ok(record) => records.push(record),
            Err(_) => {
                let event = serde_json::from_str::<OpenFangOpsEvent>(line)
                    .map_err(|err| format!("invalid outbox JSON at line {}: {err}", index + 1))?;
                records.push(OutboxRecord::new(sanitize_event(event)));
            }
        }
    }
    Ok(records)
}

fn write_outbox_records(path: &Path, records: &[OutboxRecord]) -> Result<(), String> {
    if records.is_empty() {
        match fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.to_string()),
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let tmp_path = path.with_extension("jsonl.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|err| err.to_string())?;
        for record in records {
            serde_json::to_writer(&mut file, record).map_err(|err| err.to_string())?;
            writeln!(file).map_err(|err| err.to_string())?;
        }
        file.flush().map_err(|err| err.to_string())?;
    }
    fs::rename(tmp_path, path).map_err(|err| err.to_string())
}

fn enforce_outbox_limit(records: &mut Vec<OutboxRecord>) {
    if records.len() > MAX_OUTBOX_RECORDS {
        let drop_count = records.len() - MAX_OUTBOX_RECORDS;
        warn!(
            drop_count,
            "OpenFang ops event outbox exceeded limit; dropping oldest records"
        );
        records.drain(0..drop_count);
    }
}

fn quarantine_outbox(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let quarantine_path = path.with_extension(format!("jsonl.corrupt-{}", now_ms()));
    fs::rename(path, quarantine_path).map_err(|err| err.to_string())
}

fn sanitize_event(mut event: OpenFangOpsEvent) -> OpenFangOpsEvent {
    event.severity = normalize_severity(&event.severity);
    event.source = truncate_chars(&event.source, 128);
    event.component = truncate_chars(&event.component, 128);
    event.event_type = truncate_chars(&event.event_type, 128);
    event.title = truncate_chars(&event.title, 240);
    event.message = truncate_chars(&event.message, MAX_FIELD_CHARS);
    event.technical_detail = truncate_chars(&event.technical_detail, MAX_FIELD_CHARS);
    event.impact = truncate_chars(&event.impact, MAX_FIELD_CHARS);
    event.dedupe_key = truncate_chars(&event.dedupe_key, 300);
    event.agent = truncate_chars(&event.agent, 160);
    event.run_id = truncate_chars(&event.run_id, 160);
    event.job_id = truncate_chars(&event.job_id, 160);
    event.payload_json = sanitize_json_value(event.payload_json);
    event
}

fn sanitize_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => {
            serde_json::Value::String(truncate_chars(&text, MAX_PAYLOAD_STRING_CHARS))
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .take(100)
                .map(sanitize_json_value)
                .collect(),
        ),
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .take(100)
                .map(|(key, value)| (truncate_chars(&key, 160), sanitize_json_value(value)))
                .collect(),
        ),
        other => other,
    }
}

fn format_discord_system_event_alert(event: &OpenFangOpsEvent) -> String {
    let flow = event_flow_label(event);
    let blocked_step = event_blocked_step_label(event);
    let reason = truncate_chars(
        &first_non_empty([
            event.technical_detail.as_str(),
            event.message.as_str(),
            event.title.as_str(),
        ]),
        DISCORD_ALERT_REASON_CHARS,
    );
    let impact = truncate_chars(
        &first_non_empty([
            event.impact.as_str(),
            "這個流程沒有完整完成，需要檢查後再繼續。",
        ]),
        DISCORD_ALERT_IMPACT_CHARS,
    );
    let agent = truncate_chars(
        &first_non_empty([event.agent.as_str(), "openfang-runtime"]),
        DISCORD_ALERT_AGENT_CHARS,
    );
    let job = truncate_chars(
        &first_non_empty([event.job_id.as_str(), "-"]),
        DISCORD_ALERT_JOB_CHARS,
    );
    let mut message = format!(
        "OpenFang 系統事件通知\n\
流程：{flow}\n\
卡住位置：{blocked_step}\n\
原因：{reason}\n\
影響：{impact}\n\
等級：{}\n\
Agent：{agent}\n\
Job：{job}\n\
事件：{}/{}\n\
\n\
這筆事件已寫入 OpenFang 本機 ops event store，請檢查 OpenFang 系統紀錄。",
        event.severity, event.component, event.event_type
    );
    if message.chars().count() > DISCORD_ALERT_MESSAGE_CHARS {
        message = truncate_chars(&message, DISCORD_ALERT_MESSAGE_CHARS);
    }
    message
}

fn event_flow_label(event: &OpenFangOpsEvent) -> String {
    match event.component.as_str() {
        "studio_os_tool" => {
            let action = event
                .payload_json
                .get("action")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            match action {
                "create_report" => "Studio OS 建立報告".to_string(),
                "create_agent_run" => "Studio OS 記錄 agent 執行".to_string(),
                "create" => "Studio OS 建立核心資料".to_string(),
                "update" => "Studio OS 更新核心資料".to_string(),
                "delete" => "Studio OS 刪除核心資料".to_string(),
                "review_report" => "Studio OS 報告回饋".to_string(),
                "get_summary" | "list" | "get" | "get_report_content" => {
                    "Studio OS 讀取資料".to_string()
                }
                "" => "Studio OS 工具呼叫".to_string(),
                other => format!("Studio OS 工具呼叫：{other}"),
            }
        }
        "cron" => {
            let job_name = event
                .payload_json
                .get("job_name")
                .and_then(|value| value.as_str())
                .or_else(|| {
                    event
                        .payload_json
                        .get("job_id")
                        .and_then(|value| value.as_str())
                })
                .or({
                    if event.job_id.is_empty() {
                        None
                    } else {
                        Some(event.job_id.as_str())
                    }
                })
                .unwrap_or("未命名排程");
            format!("排程：{job_name}")
        }
        "memory" => "記憶沉澱 / embedding".to_string(),
        "heartbeat" => "Agent 健康監控".to_string(),
        "" => event.event_type.clone(),
        other => format!("{other} / {}", event.event_type),
    }
}

fn event_blocked_step_label(event: &OpenFangOpsEvent) -> String {
    if event.component == "studio_os_tool" {
        let failure_type = event
            .payload_json
            .get("failure_type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let action = event
            .payload_json
            .get("action")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        return match (action, failure_type) {
            ("create_report", "http_error") => {
                "agent 已產出內容，但 Studio OS 建立報告 API 回傳錯誤".to_string()
            }
            ("create_report", "request_failed") => {
                "agent 要寫入報告時，連不到 Studio OS".to_string()
            }
            ("create_report", "response_read_failed") => {
                "agent 寫入報告後，讀取 Studio OS 回應失敗".to_string()
            }
            (_, "http_error") => "Studio OS API 回傳錯誤".to_string(),
            (_, "request_failed") => "連線到 Studio OS 失敗".to_string(),
            (_, "response_read_failed") => "讀取 Studio OS 回應失敗".to_string(),
            _ => event_flow_label(event),
        };
    }
    event_flow_label(event)
}

fn first_non_empty<const N: usize>(values: [&str; N]) -> String {
    values
        .into_iter()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or("-")
        .to_string()
}

fn validate_discord_channel_id(channel_id: &str) -> Result<(), String> {
    let len = channel_id.len();
    if (15..=25).contains(&len) && channel_id.chars().all(|ch| ch.is_ascii_digit()) {
        Ok(())
    } else {
        Err(format!("Invalid Discord channel id: {channel_id}"))
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated = text
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_round_trips_records_and_clears_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops_events_outbox.jsonl");
        let event = OpenFangOpsEvent::error("cron", "cron_failed", "Cron failed")
            .with_agent("studio-lead")
            .with_message("scheduled run failed")
            .with_dedupe_key("cron_failed:test");

        append_outbox_record(&path, OutboxRecord::new(event)).unwrap();
        let loaded = load_outbox_records(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].event.severity, "error");
        assert_eq!(loaded[0].event.dedupe_key, "cron_failed:test");

        write_outbox_records(&path, &[]).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn outbox_limit_drops_oldest_records() {
        let mut records = (0..(MAX_OUTBOX_RECORDS + 2))
            .map(|index| {
                let mut record =
                    OutboxRecord::new(OpenFangOpsEvent::warning("test", "event", "event"));
                record.id = format!("record-{index}");
                record
            })
            .collect::<Vec<_>>();
        enforce_outbox_limit(&mut records);
        assert_eq!(records.len(), MAX_OUTBOX_RECORDS);
        assert_eq!(records[0].id, "record-2");
    }

    #[test]
    fn delivery_outcomes_drop_success_and_4xx_but_keep_retry() {
        let mut delivered = OutboxRecord::new(OpenFangOpsEvent::warning("test", "ok", "ok"));
        delivered.id = "delivered".to_string();
        let mut dropped = OutboxRecord::new(OpenFangOpsEvent::warning("test", "bad", "bad"));
        dropped.id = "dropped".to_string();
        let mut retried = OutboxRecord::new(OpenFangOpsEvent::warning("test", "retry", "retry"));
        retried.id = "retried".to_string();
        let mut records = vec![delivered, dropped, retried];
        apply_delivery_outcomes(
            &mut records,
            HashMap::from([
                ("delivered".to_string(), DeliveryOutcome::Delivered),
                (
                    "dropped".to_string(),
                    DeliveryOutcome::Drop("400".to_string()),
                ),
                (
                    "retried".to_string(),
                    DeliveryOutcome::Retry("500".to_string()),
                ),
            ]),
        );
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, "retried");
        assert_eq!(records[0].attempts, 1);
        assert_eq!(records[0].last_error, "500");
    }

    #[test]
    fn outbox_batch_skips_recent_retries_so_fresh_records_are_not_starved() {
        let now = now_ms();
        let mut records = (0..FLUSH_BATCH_SIZE)
            .map(|index| {
                let mut record =
                    OutboxRecord::new(OpenFangOpsEvent::warning("test", "retry", "retry"));
                record.id = format!("retry-{index}");
                record.attempts = 1;
                record.last_attempt_at_ms = Some(now.saturating_sub(1_000));
                record
            })
            .collect::<Vec<_>>();
        let mut fresh = OutboxRecord::new(OpenFangOpsEvent::warning("test", "fresh", "fresh"));
        fresh.id = "fresh".to_string();
        records.push(fresh);

        let batch = select_outbox_batch(&records, now);

        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, "fresh");
        let stats = flush_stats_for_records(&records, now);
        assert_eq!(stats.remaining, FLUSH_BATCH_SIZE + 1);
        assert_eq!(stats.retry_count, FLUSH_BATCH_SIZE);
        assert_eq!(stats.ready_count, 1);
    }

    #[test]
    fn retry_backoff_is_per_record_and_capped() {
        let mut record = OutboxRecord::new(OpenFangOpsEvent::warning("test", "retry", "retry"));
        record.attempts = 1;
        assert_eq!(record_retry_backoff(&record), FLUSH_BACKOFF);

        record.attempts = 5;
        assert_eq!(
            record_retry_backoff(&record),
            FLUSH_BACKOFF.saturating_mul(16)
        );

        record.attempts = 6;
        assert_eq!(record_retry_backoff(&record), MAX_RECORD_RETRY_BACKOFF);
    }

    #[test]
    fn legacy_event_lines_are_migrated_to_outbox_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops_events_outbox.jsonl");
        let event = OpenFangOpsEvent::warning(
            "memory",
            EVENT_TYPE_LEGACY_EMBEDDING_FAILED,
            "Embedding failed",
        );
        fs::write(&path, serde_json::to_string(&event).unwrap()).unwrap();

        let records = load_outbox_records(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].event.event_type,
            EVENT_TYPE_LEGACY_EMBEDDING_FAILED
        );
    }

    #[test]
    fn ops_event_store_dedupes_open_events_in_openfang_db() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        let event = OpenFangOpsEvent::error("cron", "cron_failed", "Cron failed")
            .with_agent("studio-lead")
            .with_message("scheduled run failed")
            .with_detail("provider timeout")
            .with_dedupe_key("cron_failed:daily")
            .with_payload(serde_json::json!({"job_name": "daily"}));

        upsert_ops_event(&conn, &event).unwrap();
        upsert_ops_event(&conn, &event.with_detail("provider timeout again")).unwrap();

        let (count, occurrences, detail): (i64, i64, String) = conn
            .query_row(
                "SELECT COUNT(*), MAX(occurrences), MAX(technical_detail) FROM ops_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(count, 1);
        assert_eq!(occurrences, 2);
        assert_eq!(detail, "provider timeout again");
    }

    #[test]
    fn ops_event_store_keeps_highest_severity_for_deduped_events() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        let error = OpenFangOpsEvent::error("cron", "cron_failed", "Cron failed")
            .with_dedupe_key("cron_failed:severity");
        let warning = OpenFangOpsEvent::warning("cron", "cron_failed", "Cron warning")
            .with_dedupe_key("cron_failed:severity");

        upsert_ops_event(&conn, &error).unwrap();
        upsert_ops_event(&conn, &warning).unwrap();

        let severity: String = conn
            .query_row(
                "SELECT severity FROM ops_events WHERE dedupe_key = ?",
                ["cron_failed:severity"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(severity, "error");
    }

    #[test]
    fn ops_event_notification_timestamp_is_persisted() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        let event = OpenFangOpsEvent::error("cron", "cron_failed", "Cron failed")
            .with_dedupe_key("cron_failed:notified");
        upsert_ops_event(&conn, &event).unwrap();

        mark_ops_event_notified_in_connection(
            &conn,
            "cron_failed:notified",
            "2026-05-13T12:00:00Z",
        )
        .unwrap();

        let notified_at: String = conn
            .query_row(
                "SELECT last_notified_at FROM ops_events WHERE dedupe_key = ?",
                ["cron_failed:notified"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(notified_at, "2026-05-13T12:00:00Z");
    }

    #[test]
    fn discord_alerts_notify_errors_and_actionable_warnings() {
        let noisy_warning = OpenFangOpsEvent::warning(
            "heartbeat",
            "agent_unresponsive",
            "Agent temporarily unresponsive",
        );
        let embedding_warning = OpenFangOpsEvent::warning(
            "memory",
            EVENT_TYPE_MEMORY_EMBEDDING_FAILED,
            "Memory embedding failed",
        );
        let legacy_embedding_warning = OpenFangOpsEvent::warning(
            "memory",
            EVENT_TYPE_LEGACY_EMBEDDING_FAILED,
            "Embedding failed",
        );
        let delivery_warning = OpenFangOpsEvent::warning(
            "cron",
            EVENT_TYPE_CRON_DELIVERY_FAILED,
            "Cron delivery failed",
        );
        let persist_warning = OpenFangOpsEvent::warning(
            "cron",
            EVENT_TYPE_CRON_STATE_PERSIST_FAILED,
            "Cron state persist failed",
        );
        let error =
            OpenFangOpsEvent::error("studio_os_tool", "studio_os_tool_failed", "Tool failed");
        let mut critical = OpenFangOpsEvent::error("heartbeat", "agent_crashed", "Crashed");
        critical.severity = "critical".to_string();

        assert!(!should_notify_discord_alert(&noisy_warning));
        assert!(should_notify_discord_alert(&embedding_warning));
        assert!(should_notify_discord_alert(&legacy_embedding_warning));
        assert!(should_notify_discord_alert(&delivery_warning));
        assert!(should_notify_discord_alert(&persist_warning));
        assert!(should_notify_discord_alert(&error));
        assert!(should_notify_discord_alert(&critical));
    }

    #[test]
    fn discord_alert_message_explains_flow_in_traditional_chinese() {
        let event = OpenFangOpsEvent::error(
            "studio_os_tool",
            "studio_os_tool_failed",
            "Studio OS tool call failed",
        )
        .with_agent("studio-opportunity-scout")
        .with_message("The studio_os tool action 'create_report' did not complete successfully.")
        .with_detail("HTTP 403 Forbidden: unknown or unauthorized report actor")
        .with_impact("The intended Studio OS state change did not land.")
        .with_dedupe_key(
            "studio_os_tool_failed:create_report:http_error:403:studio-opportunity-scout",
        )
        .with_payload(serde_json::json!({
            "action": "create_report",
            "failure_type": "http_error",
            "status": 403
        }));

        let message = format_discord_system_event_alert(&event);

        assert!(message.contains("OpenFang 系統事件通知"));
        assert!(message.contains("流程：Studio OS 建立報告"));
        assert!(message.contains("卡住位置：agent 已產出內容"));
        assert!(message.contains("原因：HTTP 403 Forbidden"));
        assert!(message.contains("等級：error"));
        assert!(message.contains("Agent：studio-opportunity-scout"));
        assert!(message.contains("這筆事件已寫入 OpenFang 本機 ops event store"));
    }

    #[test]
    fn discord_alert_message_preserves_operational_fields_when_reason_is_long() {
        let event = OpenFangOpsEvent::error("cron", "cron_agent_turn_timeout", "Cron timed out")
            .with_agent("studio-opportunity-scout")
            .with_detail("x".repeat(DISCORD_ALERT_REASON_CHARS * 4))
            .with_impact("y".repeat(DISCORD_ALERT_IMPACT_CHARS * 4))
            .with_payload(
                serde_json::json!({"job_name": "studio-daily-opportunity-community-signals"}),
            );

        let message = format_discord_system_event_alert(&event);

        assert!(message.chars().count() <= DISCORD_ALERT_MESSAGE_CHARS);
        assert!(message.contains("等級：error"));
        assert!(message.contains("Agent：studio-opportunity-scout"));
        assert!(message.contains("事件：cron/cron_agent_turn_timeout"));
    }

    #[test]
    fn discord_alert_outbox_suppresses_pending_duplicate_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alerts.jsonl");
        let state_path = dir.path().join("alerts_state.json");
        let event = OpenFangOpsEvent::error("cron", "cron_failed", "Cron failed")
            .with_dedupe_key("cron_failed:daily");

        append_discord_alert_record_with_state(
            &path,
            &state_path,
            OutboxRecord::new(event.clone()),
        )
        .unwrap();
        append_discord_alert_record_with_state(&path, &state_path, OutboxRecord::new(event))
            .unwrap();

        let records = load_outbox_records(&path).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn validates_discord_channel_ids() {
        assert!(validate_discord_channel_id("1503357433425301564").is_ok());
        assert!(validate_discord_channel_id("../studio.db").is_err());
        assert!(validate_discord_channel_id("abc").is_err());
    }

    #[test]
    fn reads_secret_from_dotenv_style_file_without_logging_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".env");
        fs::write(
            &path,
            "OTHER=value\nDISCORD_BOT_TOKEN=\"secret-token\"\nEMPTY=\n",
        )
        .unwrap();

        assert_eq!(
            read_secret_from_file(&path, "DISCORD_BOT_TOKEN").unwrap(),
            Some("secret-token".to_string())
        );
        assert_eq!(read_secret_from_file(&path, "EMPTY").unwrap(), None);
        assert_eq!(read_secret_from_file(&path, "MISSING").unwrap(), None);
    }
}
