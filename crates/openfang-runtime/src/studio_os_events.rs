//! Durable Studio OS system event sink.
//!
//! Runtime warnings must not depend on an agent deciding to report them, and
//! observability must not block cron, heartbeat, or tool execution paths. Calls
//! to `record_system_event` only enqueue a bounded local outbox item; a
//! detached worker flushes the outbox to Studio OS with backoff.

use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::warn;

const STUDIO_OS_DEFAULT_BASE_URL: &str = "http://127.0.0.1:4310";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const FLUSH_BACKOFF: Duration = Duration::from_secs(30);
const MAX_RECORD_RETRY_BACKOFF: Duration = Duration::from_secs(15 * 60);
const MAX_OUTBOX_RECORDS: usize = 1_000;
const FLUSH_BATCH_SIZE: usize = 50;
const MAX_FIELD_CHARS: usize = 8_000;
const MAX_PAYLOAD_STRING_CHARS: usize = 4_000;

static OUTBOX_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));
static FLUSH_RUNNING: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StudioOsSystemEvent {
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
    event: StudioOsSystemEvent,
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

impl StudioOsSystemEvent {
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

pub async fn record_system_event(event: StudioOsSystemEvent) {
    let path = outbox_path();
    {
        let _guard = OUTBOX_LOCK.lock().await;
        if let Err(err) = append_outbox_record(&path, OutboxRecord::new(event)) {
            warn!(path = ?path, "Studio OS system event outbox append failed: {err}");
            return;
        }
    }
    schedule_outbox_flush();
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
                warn!("Studio OS system event outbox flush failed: {err}");
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
    let (base_url, token) = event_sink_config()?;
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|err| format!("Studio OS system event client build failed: {err}"))?;
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
        let outcome = post_system_event(&client, &base_url, &token, &record.event).await;
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
            warn!(path = ?path, "Studio OS system event outbox check failed: {err}");
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

async fn post_system_event(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    event: &StudioOsSystemEvent,
) -> DeliveryOutcome {
    let mut body = match serde_json::to_value(event) {
        Ok(body) => body,
        Err(err) => return DeliveryOutcome::Drop(format!("event serialization failed: {err}")),
    };
    let Some(map) = body.as_object_mut() else {
        return DeliveryOutcome::Drop("system event did not serialize to an object".to_string());
    };
    map.insert(
        "actor".to_string(),
        serde_json::Value::String("openfang-runtime".to_string()),
    );

    let url = format!("{base_url}/api/system_events");
    let response = match client
        .post(url)
        .header("X-Studio-OS-Token", token)
        .json(&body)
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
        "Studio OS returned HTTP {status}: {}",
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
                warn!(record_id = %id, reason = %reason, "Dropping rejected Studio OS system event");
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

fn event_sink_config() -> Result<(String, String), String> {
    let base_url = studio_os_base_url()?;
    let token = studio_os_write_token()?;
    Ok((base_url, token))
}

fn studio_os_base_url() -> Result<String, String> {
    let raw =
        std::env::var("STUDIO_OS_URL").unwrap_or_else(|_| STUDIO_OS_DEFAULT_BASE_URL.to_string());
    let mut url =
        reqwest::Url::parse(&raw).map_err(|err| format!("Invalid STUDIO_OS_URL: {err}"))?;
    let host = url.host_str().ok_or("STUDIO_OS_URL must include a host")?;
    // System events may include operational details from the local runtime.
    // Keep this sink loopback-only unless Studio OS gains a remote deployment
    // story with transport security, authentication, and payload redaction.
    if url.scheme() != "http" || !matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return Err("STUDIO_OS_URL must be an http loopback URL".to_string());
    }
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("STUDIO_OS_URL must not include credentials, query, or fragment".to_string());
    }
    if !matches!(url.path(), "" | "/") {
        return Err("STUDIO_OS_URL must point to the Studio OS server root".to_string());
    }
    url.set_path("");
    Ok(url.to_string().trim_end_matches('/').to_string())
}

fn studio_os_write_token() -> Result<String, String> {
    if let Ok(token) = std::env::var("STUDIO_OS_WRITE_TOKEN") {
        let token = token.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }

    let token_file = studio_os_write_token_file();
    let token = std::fs::read_to_string(&token_file)
        .map_err(|err| format!("Unable to read Studio OS write token file {token_file:?}: {err}"))?
        .trim()
        .to_string();
    if token.is_empty() {
        Err(format!(
            "Studio OS write token file {token_file:?} is empty"
        ))
    } else {
        Ok(token)
    }
}

fn studio_os_write_token_file() -> PathBuf {
    if let Ok(path) = std::env::var("STUDIO_OS_WRITE_TOKEN_FILE") {
        return PathBuf::from(path);
    }
    if let Ok(root) = std::env::var("STUDIO_OS_ROOT") {
        return PathBuf::from(root).join(".studio_os_write_token");
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home)
            .join("studio-os")
            .join(".studio_os_write_token");
    }
    PathBuf::from(".studio_os_write_token")
}

fn outbox_path() -> PathBuf {
    openfang_home_dir().join("system_events_outbox.jsonl")
}

fn openfang_home_dir() -> PathBuf {
    if let Ok(home) = std::env::var("OPENFANG_HOME") {
        return PathBuf::from(home);
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home).join(".openfang");
    }
    PathBuf::from(".openfang")
}

impl OutboxRecord {
    fn new(event: StudioOsSystemEvent) -> Self {
        Self {
            id: format!("{}-{}-{}", now_ms(), std::process::id(), random_suffix()),
            event: sanitize_event(event),
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
            warn!(path = ?path, "Quarantined invalid Studio OS outbox before appending: {err}");
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
                let event = serde_json::from_str::<StudioOsSystemEvent>(line)
                    .map_err(|err| format!("invalid outbox JSON at line {}: {err}", index + 1))?;
                records.push(OutboxRecord::new(event));
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
            "Studio OS system event outbox exceeded limit; dropping oldest records"
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

fn sanitize_event(mut event: StudioOsSystemEvent) -> StudioOsSystemEvent {
    event.severity = truncate_chars(&event.severity, 32);
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

fn random_suffix() -> String {
    format!(
        "{:016x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            ^ u128::from(std::process::id())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_round_trips_records_and_clears_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system_events_outbox.jsonl");
        let event = StudioOsSystemEvent::error("cron", "cron_failed", "Cron failed")
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
                    OutboxRecord::new(StudioOsSystemEvent::warning("test", "event", "event"));
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
        let mut delivered = OutboxRecord::new(StudioOsSystemEvent::warning("test", "ok", "ok"));
        delivered.id = "delivered".to_string();
        let mut dropped = OutboxRecord::new(StudioOsSystemEvent::warning("test", "bad", "bad"));
        dropped.id = "dropped".to_string();
        let mut retried = OutboxRecord::new(StudioOsSystemEvent::warning("test", "retry", "retry"));
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
                    OutboxRecord::new(StudioOsSystemEvent::warning("test", "retry", "retry"));
                record.id = format!("retry-{index}");
                record.attempts = 1;
                record.last_attempt_at_ms = Some(now.saturating_sub(1_000));
                record
            })
            .collect::<Vec<_>>();
        let mut fresh = OutboxRecord::new(StudioOsSystemEvent::warning("test", "fresh", "fresh"));
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
        let mut record = OutboxRecord::new(StudioOsSystemEvent::warning("test", "retry", "retry"));
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
        let path = dir.path().join("system_events_outbox.jsonl");
        let event = StudioOsSystemEvent::warning("memory", "embedding_failed", "Embedding failed");
        fs::write(&path, serde_json::to_string(&event).unwrap()).unwrap();

        let records = load_outbox_records(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event.event_type, "embedding_failed");
    }
}
