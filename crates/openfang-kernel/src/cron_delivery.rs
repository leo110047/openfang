//! Multi-destination cron output delivery.
//!
//! A single [`CronJob`] may declare zero or more [`CronDeliveryTarget`]s on
//! its `delivery_targets` field. After the job fires and produces output,
//! the [`CronDeliveryEngine`] fans out the same payload to every target
//! concurrently. Failures in one target do not abort delivery to the
//! others — every target's outcome is returned in a [`DeliveryResult`].
//!
//! This is the OpenFang port of the Hermes Agent multi-destination cron
//! pattern: one job → N destinations (channels / webhooks / files / email).

use futures::future::join_all;
use openfang_channels::bridge::ChannelBridgeHandle;
use openfang_types::scheduler::CronDeliveryTarget;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Webhook HTTP timeout. Matches the legacy single-target cron webhook.
const WEBHOOK_TIMEOUT_SECS: u64 = 30;

/// Per-target delivery outcome returned by [`CronDeliveryEngine::deliver`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryResult {
    /// Human-readable target description (`"channel:telegram -> chat_123"`,
    /// `"webhook:https://..."`, `"file:/tmp/out.log"`, `"email:alice@x"`).
    pub target: String,
    /// Whether delivery succeeded.
    pub success: bool,
    /// Error message if `success` is `false`.
    pub error: Option<String>,
}

impl DeliveryResult {
    fn ok(target: String) -> Self {
        Self {
            target,
            success: true,
            error: None,
        }
    }

    fn err(target: String, msg: String) -> Self {
        Self {
            target,
            success: false,
            error: Some(msg),
        }
    }
}

/// Fan-out delivery engine for cron job output.
///
/// Holds a reference to the channel bridge (for adapter-based delivery) and
/// a shared HTTP client (for webhook delivery). Constructed once per kernel
/// and reused across every cron firing.
pub struct CronDeliveryEngine {
    /// Bridge used to invoke `send_channel_message` on registered adapters.
    channel_bridge: Arc<dyn ChannelBridgeHandle>,
    /// Shared HTTP client for webhook delivery.
    http: reqwest::Client,
}

impl CronDeliveryEngine {
    /// Build a new engine using the given channel bridge and a fresh
    /// `reqwest::Client`. Falls back to the default client if the builder
    /// fails (which effectively never happens on supported platforms).
    pub fn new(channel_bridge: Arc<dyn ChannelBridgeHandle>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(WEBHOOK_TIMEOUT_SECS))
            .build()
            .unwrap_or_default();
        Self {
            channel_bridge,
            http,
        }
    }

    /// Build a new engine with an explicit HTTP client — used by tests.
    pub fn with_http_client(
        channel_bridge: Arc<dyn ChannelBridgeHandle>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            channel_bridge,
            http,
        }
    }

    /// Deliver `output` to every target concurrently.
    ///
    /// Returns a `Vec<DeliveryResult>` with one entry per target in the same
    /// order as the input slice. One target failing does not short-circuit
    /// the others — the job already succeeded, delivery is best-effort.
    pub async fn deliver(
        &self,
        targets: &[CronDeliveryTarget],
        job_name: &str,
        output: &str,
    ) -> Vec<DeliveryResult> {
        if targets.is_empty() {
            return Vec::new();
        }
        let futures = targets
            .iter()
            .map(|t| self.deliver_one(t, job_name, output));
        join_all(futures).await
    }

    /// Deliver to a single target. Never panics.
    async fn deliver_one(
        &self,
        target: &CronDeliveryTarget,
        job_name: &str,
        output: &str,
    ) -> DeliveryResult {
        match target {
            CronDeliveryTarget::Channel {
                channel_type,
                recipient,
            } => {
                let desc = format!("channel:{channel_type} -> {recipient}");
                match self
                    .channel_bridge
                    .send_channel_message(channel_type, recipient, output)
                    .await
                {
                    Ok(()) => {
                        debug!(target = %desc, "Cron fan-out: channel delivery ok");
                        DeliveryResult::ok(desc)
                    }
                    Err(e) => {
                        warn!(target = %desc, error = %e, "Cron fan-out: channel delivery failed");
                        DeliveryResult::err(desc, e)
                    }
                }
            }
            CronDeliveryTarget::Webhook { url, auth_header } => {
                let desc = format!("webhook:{url}");
                match deliver_webhook(&self.http, url, auth_header.as_deref(), job_name, output)
                    .await
                {
                    Ok(()) => {
                        debug!(target = %desc, "Cron fan-out: webhook delivery ok");
                        DeliveryResult::ok(desc)
                    }
                    Err(e) => {
                        warn!(target = %desc, error = %e, "Cron fan-out: webhook delivery failed");
                        DeliveryResult::err(desc, e)
                    }
                }
            }
            CronDeliveryTarget::LocalFile { path, append } => {
                let desc = format!("file:{path}");
                match deliver_local_file(Path::new(path), *append, output).await {
                    Ok(()) => {
                        debug!(target = %desc, "Cron fan-out: file write ok");
                        DeliveryResult::ok(desc)
                    }
                    Err(e) => {
                        warn!(target = %desc, error = %e, "Cron fan-out: file write failed");
                        DeliveryResult::err(desc, e)
                    }
                }
            }
            CronDeliveryTarget::Email {
                to,
                subject_template,
            } => {
                let desc = format!("email:{to}");
                let subject = render_subject(subject_template.as_deref(), job_name);
                // The existing email channel adapter sends via SMTP and does
                // not expose a subject/to pair on the trait, so we route a
                // formatted message through it. Most adapters treat the
                // recipient as a destination identifier; the email adapter
                // uses it as the RCPT TO address.
                let body = format!("{subject}\n\n{output}");
                match self
                    .channel_bridge
                    .send_channel_message("email", to, &body)
                    .await
                {
                    Ok(()) => {
                        debug!(target = %desc, "Cron fan-out: email delivery ok");
                        DeliveryResult::ok(desc)
                    }
                    Err(e) => {
                        warn!(target = %desc, error = %e, "Cron fan-out: email delivery failed");
                        DeliveryResult::err(desc, e)
                    }
                }
            }
            CronDeliveryTarget::StudioOsReport {
                base_url,
                actor,
                report_type,
                title_template,
                summary,
                token_env: _,
                token_file: _,
            } => {
                let desc = format!("studio_os_report:{base_url}");
                let opts = StudioOsReportDelivery {
                    base_url,
                    actor,
                    report_type: report_type.as_deref(),
                    title_template: title_template.as_deref(),
                    summary: summary.as_deref(),
                };
                match deliver_studio_os_report(&self.http, opts, job_name, output).await {
                    Ok(report_ref) => {
                        debug!(target = %desc, report = %report_ref, "Cron fan-out: Studio OS report delivery ok");
                        DeliveryResult::ok(format!("{desc} -> {report_ref}"))
                    }
                    Err(e) => {
                        warn!(target = %desc, error = %e, "Cron fan-out: Studio OS report delivery failed");
                        DeliveryResult::err(desc, e)
                    }
                }
            }
        }
    }
}

/// Render an email subject from an optional template. `{job}` is the only
/// supported placeholder; everything else passes through unchanged.
fn render_subject(template: Option<&str>, job_name: &str) -> String {
    match template {
        Some(t) if !t.is_empty() => t.replace("{job}", job_name),
        _ => format!("Cron: {job_name}"),
    }
}

/// POST a JSON payload `{ job, output, timestamp }` to `url` and optionally
/// attach an `Authorization` header. Returns `Err(msg)` on non-2xx or
/// network failure.
async fn deliver_webhook(
    http: &reqwest::Client,
    url: &str,
    auth_header: Option<&str>,
    job_name: &str,
    output: &str,
) -> Result<(), String> {
    let payload = serde_json::json!({
        "job": job_name,
        "output": output,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    let mut req = http.post(url).json(&payload);
    if let Some(auth) = auth_header {
        req = req.header("Authorization", auth);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("webhook send failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("webhook returned HTTP {status}"));
    }
    Ok(())
}

struct StudioOsReportDelivery<'a> {
    base_url: &'a str,
    actor: &'a str,
    report_type: Option<&'a str>,
    title_template: Option<&'a str>,
    summary: Option<&'a str>,
}

/// Persist a cron output as a Studio OS report by POSTing to `/api/reports`.
///
/// Studio OS is a local operational database. To avoid leaking its write token,
/// this delivery target only allows loopback hosts.
async fn deliver_studio_os_report(
    http: &reqwest::Client,
    opts: StudioOsReportDelivery<'_>,
    job_name: &str,
    output: &str,
) -> Result<String, String> {
    let endpoint = studio_os_reports_endpoint(opts.base_url)?;
    let token = resolve_studio_os_token().await?;
    let title = render_report_title(opts.title_template, job_name);
    let report_type = non_empty(opts.report_type).unwrap_or("general");
    let summary = non_empty(opts.summary)
        .map(str::to_string)
        .unwrap_or_else(|| derive_report_summary(output));
    let actor = opts.actor.trim();
    let actor = if actor.is_empty() {
        "openfang-runtime"
    } else {
        actor
    };
    let payload = serde_json::json!({
        "actor": actor,
        "title": title,
        "type": report_type,
        "summary": summary,
        "content": output,
    });

    let resp = http
        .post(endpoint)
        .header("X-Studio-OS-Token", token)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("Studio OS report send failed: {e}"))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| format!("Studio OS report response read failed: {e}"))?;
    if !status.is_success() {
        return Err(format!(
            "Studio OS returned HTTP {status}: {}",
            truncate_for_error(&body, 300)
        ));
    }
    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| format!("Studio OS returned invalid JSON: {e}"))?;
    let id = parsed["id"].as_str().unwrap_or("unknown-report");
    let path = parsed["path"].as_str().unwrap_or("");
    if path.is_empty() {
        Ok(id.to_string())
    } else {
        Ok(format!("{id} ({path})"))
    }
}

fn studio_os_reports_endpoint(raw: &str) -> Result<reqwest::Url, String> {
    let mut parsed = normalize_studio_os_base_url(raw)?;
    parsed.set_path("/api/reports");
    Ok(parsed)
}

fn normalize_studio_os_base_url(raw: &str) -> Result<reqwest::Url, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("Studio OS base_url must not be empty".to_string());
    }
    let parsed =
        reqwest::Url::parse(trimmed).map_err(|e| format!("invalid Studio OS base_url: {e}"))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err("Studio OS base_url must use http or https".to_string());
    }
    if parsed.username() != "" || parsed.password().is_some() {
        return Err("Studio OS base_url must not contain credentials".to_string());
    }
    if parsed.path() != "/" {
        return Err("Studio OS base_url must not contain a path".to_string());
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err("Studio OS base_url must not contain query or fragment".to_string());
    }
    let host = parsed.host_str().unwrap_or("");
    if host != "127.0.0.1" && host != "localhost" && host != "::1" {
        return Err("Studio OS base_url must point to a loopback host".to_string());
    }
    #[cfg(not(test))]
    if parsed.port() != Some(4310) {
        return Err("Studio OS base_url must use port 4310".to_string());
    }
    #[cfg(test)]
    if parsed.port().is_none() {
        return Err("Studio OS base_url must include an explicit port".to_string());
    }
    Ok(parsed)
}

async fn resolve_studio_os_token() -> Result<String, String> {
    if let Ok(value) = std::env::var("STUDIO_OS_WRITE_TOKEN") {
        let token = value.trim();
        if !token.is_empty() {
            return Ok(token.to_string());
        }
    }

    if let Ok(path) = std::env::var("STUDIO_OS_WRITE_TOKEN_FILE") {
        let path = path.trim();
        if !path.is_empty() {
            let token = tokio::fs::read_to_string(path)
                .await
                .map_err(|e| format!("read STUDIO_OS_WRITE_TOKEN_FILE failed: {e}"))?;
            let token = token.trim();
            if !token.is_empty() {
                return Ok(token.to_string());
            }
        }
    }

    if let Some(home) = dirs::home_dir() {
        let default_path = home.join("studio-os/.studio_os_write_token");
        if default_path.exists() {
            let token = tokio::fs::read_to_string(&default_path)
                .await
                .map_err(|e| format!("read default Studio OS token file failed: {e}"))?;
            let token = token.trim();
            if !token.is_empty() {
                return Ok(token.to_string());
            }
        }
    }

    Err(
        "Studio OS write token missing; set STUDIO_OS_WRITE_TOKEN or STUDIO_OS_WRITE_TOKEN_FILE"
            .to_string(),
    )
}

fn render_report_title(template: Option<&str>, job_name: &str) -> String {
    let now = chrono::Local::now();
    let date = now.format("%Y-%m-%d").to_string();
    let timestamp = now.to_rfc3339();
    let template = non_empty(template).unwrap_or("Cron: {job} ({date})");
    render_template_once(template, job_name, &date, &timestamp)
}

fn derive_report_summary(output: &str) -> String {
    for line in output.lines() {
        let trimmed = trim_markdown_summary_prefix(line);
        if !trimmed.is_empty() {
            return truncate_for_error(trimmed, 240);
        }
    }
    "Cron output".to_string()
}

fn render_template_once(template: &str, job_name: &str, date: &str, timestamp: &str) -> String {
    let mut out = String::with_capacity(template.len() + job_name.len());
    let mut rest = template;
    while !rest.is_empty() {
        if let Some(next) = rest.strip_prefix("{job}") {
            out.push_str(job_name);
            rest = next;
        } else if let Some(next) = rest.strip_prefix("{date}") {
            out.push_str(date);
            rest = next;
        } else if let Some(next) = rest.strip_prefix("{timestamp}") {
            out.push_str(timestamp);
            rest = next;
        } else {
            let ch = rest.chars().next().expect("rest is not empty");
            out.push(ch);
            rest = &rest[ch.len_utf8()..];
        }
    }
    out
}

fn trim_markdown_summary_prefix(mut value: &str) -> &str {
    loop {
        let trimmed = value.trim_start();
        let stripped = trimmed
            .trim_start_matches(['#', '-', '*', '>'])
            .trim_start();
        if stripped.len() == trimmed.len() {
            return trimmed.trim();
        }
        value = stripped;
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

fn truncate_for_error(value: &str, max_chars: usize) -> String {
    let mut out: String = value.chars().take(max_chars).collect();
    if value.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

/// Append or overwrite `output` at `path`. Creates parent directories when
/// missing. Returns `Err(msg)` on any I/O failure.
async fn deliver_local_file(path: &Path, append: bool, output: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("create parent dir failed: {e}"))?;
        }
    }
    if append {
        use tokio::io::AsyncWriteExt;
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| format!("open failed: {e}"))?;
        f.write_all(output.as_bytes())
            .await
            .map_err(|e| format!("write failed: {e}"))?;
        // Newline separator between runs makes tailing nicer.
        f.write_all(b"\n")
            .await
            .map_err(|e| format!("write newline failed: {e}"))?;
    } else {
        tokio::fs::write(path, output.as_bytes())
            .await
            .map_err(|e| format!("write failed: {e}"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use openfang_channels::bridge::ChannelBridgeHandle;
    use openfang_types::agent::AgentId;
    use std::sync::Mutex;

    /// Mock bridge that records every channel send. Optionally fails for
    /// specific channel names.
    struct MockBridge {
        calls: Mutex<Vec<(String, String, String)>>,
        fail_on_channel: Option<String>,
    }

    impl MockBridge {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                fail_on_channel: None,
            })
        }

        fn failing_on(channel: &str) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                fail_on_channel: Some(channel.to_string()),
            })
        }

        fn calls(&self) -> Vec<(String, String, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ChannelBridgeHandle for MockBridge {
        async fn send_message(&self, _: AgentId, _: &str) -> Result<String, String> {
            Ok(String::new())
        }
        async fn find_agent_by_name(&self, _: &str) -> Result<Option<AgentId>, String> {
            Ok(None)
        }
        async fn list_agents(&self) -> Result<Vec<(AgentId, String)>, String> {
            Ok(Vec::new())
        }
        async fn spawn_agent_by_name(&self, _: &str) -> Result<AgentId, String> {
            Err("not implemented".into())
        }

        async fn send_channel_message(
            &self,
            channel_type: &str,
            recipient: &str,
            message: &str,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push((
                channel_type.to_string(),
                recipient.to_string(),
                message.to_string(),
            ));
            if let Some(ref failing) = self.fail_on_channel {
                if failing == channel_type {
                    return Err(format!("mock: forced failure on '{channel_type}'"));
                }
            }
            Ok(())
        }
    }

    fn test_engine(bridge: Arc<MockBridge>) -> CronDeliveryEngine {
        CronDeliveryEngine::new(bridge)
    }

    // -- LocalFile: overwrite ------------------------------------------------

    #[tokio::test]
    async fn localfile_overwrite_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("out.txt");
        let target = CronDeliveryTarget::LocalFile {
            path: path.to_string_lossy().to_string(),
            append: false,
        };

        let engine = test_engine(MockBridge::new());
        let results = engine.deliver(&[target], "job-x", "hello world").await;

        assert_eq!(results.len(), 1);
        assert!(results[0].success, "error: {:?}", results[0].error);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "hello world");
    }

    #[tokio::test]
    async fn localfile_overwrite_replaces_existing() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("replace.txt");
        std::fs::write(&path, "OLD CONTENT").unwrap();

        let target = CronDeliveryTarget::LocalFile {
            path: path.to_string_lossy().to_string(),
            append: false,
        };
        let engine = test_engine(MockBridge::new());
        let results = engine.deliver(&[target], "job-x", "NEW").await;

        assert!(results[0].success);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "NEW");
    }

    // -- LocalFile: append ---------------------------------------------------

    #[tokio::test]
    async fn localfile_append_adds_lines() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("log.txt");
        let target = CronDeliveryTarget::LocalFile {
            path: path.to_string_lossy().to_string(),
            append: true,
        };
        let engine = test_engine(MockBridge::new());

        // Two sequential deliveries should accumulate.
        engine
            .deliver(std::slice::from_ref(&target), "job", "first")
            .await;
        engine.deliver(&[target], "job", "second").await;

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(
            content.contains("first") && content.contains("second"),
            "expected both lines in appended file, got: {content:?}"
        );
    }

    #[tokio::test]
    async fn localfile_append_creates_missing_parent_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nested/deep/out.log");
        let target = CronDeliveryTarget::LocalFile {
            path: path.to_string_lossy().to_string(),
            append: true,
        };
        let engine = test_engine(MockBridge::new());
        let results = engine.deliver(&[target], "job", "payload").await;

        assert!(results[0].success, "error: {:?}", results[0].error);
        assert!(path.exists(), "nested file should have been created");
    }

    // -- Webhook: success ----------------------------------------------------

    #[tokio::test]
    async fn webhook_sends_payload() {
        let (port, rx) = spawn_mock_http_server(200, "OK").await;
        let url = format!("http://127.0.0.1:{port}/hook");

        let target = CronDeliveryTarget::Webhook {
            url: url.clone(),
            auth_header: Some("Bearer test-token".to_string()),
        };
        let engine = test_engine(MockBridge::new());
        let results = engine
            .deliver(&[target], "daily-report", "result body")
            .await;

        assert!(results[0].success, "error: {:?}", results[0].error);

        let captured = rx.await.expect("mock server never received a request");
        assert!(
            captured.body.contains("\"job\":\"daily-report\""),
            "payload missing job name, got: {}",
            captured.body
        );
        assert!(
            captured.body.contains("\"output\":\"result body\""),
            "payload missing output, got: {}",
            captured.body
        );
        assert!(
            captured.body.contains("\"timestamp\""),
            "payload missing timestamp, got: {}",
            captured.body
        );
        assert!(
            captured
                .headers
                .iter()
                .any(|h| h.eq_ignore_ascii_case("authorization: Bearer test-token")),
            "missing auth header, got: {:?}",
            captured.headers
        );
    }

    #[tokio::test]
    async fn webhook_reports_non_2xx() {
        let (port, _rx) = spawn_mock_http_server(500, "Internal Server Error").await;
        let url = format!("http://127.0.0.1:{port}/hook");

        let target = CronDeliveryTarget::Webhook {
            url,
            auth_header: None,
        };
        let engine = test_engine(MockBridge::new());
        let results = engine.deliver(&[target], "job", "output").await;

        assert!(!results[0].success);
        let err = results[0].error.as_deref().unwrap_or("");
        assert!(err.contains("500"), "expected 500 in error, got: {err}");
    }

    // -- Channel target ------------------------------------------------------

    #[tokio::test]
    async fn channel_target_invokes_bridge() {
        let bridge = MockBridge::new();
        let engine = test_engine(bridge.clone());
        let target = CronDeliveryTarget::Channel {
            channel_type: "slack".to_string(),
            recipient: "C12345".to_string(),
        };
        let results = engine.deliver(&[target], "alerts", "fire").await;
        assert!(results[0].success, "error: {:?}", results[0].error);
        let calls = bridge.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "slack");
        assert_eq!(calls[0].1, "C12345");
        assert_eq!(calls[0].2, "fire");
    }

    // -- Mixed success/failure ----------------------------------------------

    #[tokio::test]
    async fn mixed_targets_one_success_one_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let ok_path = tmp.path().join("ok.txt");

        let targets = vec![
            // Will succeed (file write).
            CronDeliveryTarget::LocalFile {
                path: ok_path.to_string_lossy().to_string(),
                append: false,
            },
            // Will fail (mock bridge rejects 'slack').
            CronDeliveryTarget::Channel {
                channel_type: "slack".to_string(),
                recipient: "C1".to_string(),
            },
        ];

        let bridge = MockBridge::failing_on("slack");
        let engine = test_engine(bridge);
        let results = engine.deliver(&targets, "job", "payload").await;

        assert_eq!(results.len(), 2);
        assert!(
            results[0].success,
            "file delivery should succeed: {:?}",
            results[0].error
        );
        assert!(
            !results[1].success,
            "channel delivery should fail, but got success"
        );
        assert!(results[1]
            .error
            .as_deref()
            .unwrap_or("")
            .contains("forced failure"));

        // File was still written even though the other target failed.
        assert_eq!(std::fs::read_to_string(&ok_path).unwrap(), "payload");
    }

    #[tokio::test]
    async fn empty_targets_returns_empty_vec() {
        let engine = test_engine(MockBridge::new());
        let results = engine.deliver(&[], "job", "x").await;
        assert!(results.is_empty());
    }

    // -- Serde round-trip ---------------------------------------------------

    #[test]
    fn serde_roundtrip_channel() {
        let t = CronDeliveryTarget::Channel {
            channel_type: "telegram".into(),
            recipient: "12345".into(),
        };
        let s = serde_json::to_string(&t).unwrap();
        assert!(s.contains("\"type\":\"channel\""), "tag missing: {s}");
        assert!(s.contains("telegram"));
        let back: CronDeliveryTarget = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn serde_roundtrip_webhook() {
        let t = CronDeliveryTarget::Webhook {
            url: "https://example.com/hook".into(),
            auth_header: Some("Bearer x".into()),
        };
        let s = serde_json::to_string(&t).unwrap();
        assert!(s.contains("\"type\":\"webhook\""), "tag missing: {s}");
        let back: CronDeliveryTarget = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn serde_roundtrip_webhook_without_auth() {
        // auth_header should default to None when omitted.
        let json = r#"{"type":"webhook","url":"https://x.test/h"}"#;
        let back: CronDeliveryTarget = serde_json::from_str(json).unwrap();
        assert_eq!(
            back,
            CronDeliveryTarget::Webhook {
                url: "https://x.test/h".into(),
                auth_header: None,
            }
        );
    }

    #[test]
    fn serde_roundtrip_localfile() {
        let t = CronDeliveryTarget::LocalFile {
            path: "/var/log/cron-out.log".into(),
            append: true,
        };
        let s = serde_json::to_string(&t).unwrap();
        assert!(s.contains("\"type\":\"local_file\""), "tag missing: {s}");
        let back: CronDeliveryTarget = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn serde_roundtrip_localfile_default_append() {
        // append should default to false when omitted.
        let json = r#"{"type":"local_file","path":"/tmp/out.log"}"#;
        let back: CronDeliveryTarget = serde_json::from_str(json).unwrap();
        assert_eq!(
            back,
            CronDeliveryTarget::LocalFile {
                path: "/tmp/out.log".into(),
                append: false,
            }
        );
    }

    #[test]
    fn serde_roundtrip_email() {
        let t = CronDeliveryTarget::Email {
            to: "alice@example.com".into(),
            subject_template: Some("Report: {job}".into()),
        };
        let s = serde_json::to_string(&t).unwrap();
        assert!(s.contains("\"type\":\"email\""), "tag missing: {s}");
        let back: CronDeliveryTarget = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn serde_roundtrip_studio_os_report_with_defaults() {
        let json = r#"{"type":"studio_os_report"}"#;
        let back: CronDeliveryTarget = serde_json::from_str(json).unwrap();
        assert_eq!(
            back,
            CronDeliveryTarget::StudioOsReport {
                base_url: "http://127.0.0.1:4310".into(),
                actor: "openfang-runtime".into(),
                report_type: None,
                title_template: None,
                summary: None,
                token_env: None,
                token_file: None,
            }
        );
    }

    #[tokio::test]
    async fn studio_os_report_sends_payload() {
        let (port, rx) =
            spawn_mock_http_server(201, r#"{"id":"report-123","path":"/tmp/r.md"}"#).await;
        let tmp = tempfile::tempdir().unwrap();
        let token_path = tmp.path().join("studio-token");
        std::fs::write(&token_path, "test-token\n").unwrap();
        let previous_token = std::env::var_os("STUDIO_OS_WRITE_TOKEN");
        let previous_token_file = std::env::var_os("STUDIO_OS_WRITE_TOKEN_FILE");
        std::env::remove_var("STUDIO_OS_WRITE_TOKEN");
        std::env::set_var("STUDIO_OS_WRITE_TOKEN_FILE", &token_path);

        let target = CronDeliveryTarget::StudioOsReport {
            base_url: format!("http://127.0.0.1:{port}"),
            actor: "openfang-runtime".to_string(),
            report_type: Some("daily_brief".to_string()),
            title_template: Some("Daily {job} {date}".to_string()),
            summary: Some("Morning brief".to_string()),
            token_env: None,
            token_file: None,
        };
        let engine = test_engine(MockBridge::new());
        let results = engine
            .deliver(&[target], "studio-brief", "# Brief\n\nAll good")
            .await;
        match previous_token {
            Some(value) => std::env::set_var("STUDIO_OS_WRITE_TOKEN", value),
            None => std::env::remove_var("STUDIO_OS_WRITE_TOKEN"),
        }
        match previous_token_file {
            Some(value) => std::env::set_var("STUDIO_OS_WRITE_TOKEN_FILE", value),
            None => std::env::remove_var("STUDIO_OS_WRITE_TOKEN_FILE"),
        }

        assert!(results[0].success, "error: {:?}", results[0].error);
        let captured = rx.await.expect("mock server never received a request");
        assert!(
            captured.request_line.starts_with("POST /api/reports "),
            "wrong request line: {}",
            captured.request_line
        );
        assert!(
            captured
                .headers
                .iter()
                .any(|h| h.eq_ignore_ascii_case("x-studio-os-token: test-token")),
            "missing Studio OS token header, got: {:?}",
            captured.headers
        );
        assert!(
            captured.body.contains("\"actor\":\"openfang-runtime\""),
            "payload missing actor, got: {}",
            captured.body
        );
        assert!(
            captured.body.contains("\"type\":\"daily_brief\""),
            "payload missing report type, got: {}",
            captured.body
        );
        assert!(
            captured
                .body
                .contains("\"content\":\"# Brief\\n\\nAll good\""),
            "payload missing content, got: {}",
            captured.body
        );
    }

    #[tokio::test]
    async fn studio_os_report_requires_loopback_base_url() {
        let target = CronDeliveryTarget::StudioOsReport {
            base_url: "https://example.com".to_string(),
            actor: "openfang-runtime".to_string(),
            report_type: None,
            title_template: None,
            summary: None,
            token_env: None,
            token_file: None,
        };
        let engine = test_engine(MockBridge::new());
        let results = engine.deliver(&[target], "job", "body").await;

        assert!(!results[0].success);
        let err = results[0].error.as_deref().unwrap_or("");
        assert!(
            err.contains("loopback"),
            "expected loopback validation error, got: {err}"
        );
    }

    #[test]
    fn studio_os_report_rejects_path_query_and_fragment_base_url() {
        for base_url in [
            "http://127.0.0.1:4310/foo",
            "http://127.0.0.1:4310?x=1",
            "http://127.0.0.1:4310#fragment",
        ] {
            assert!(
                normalize_studio_os_base_url(base_url).is_err(),
                "base_url should be rejected: {base_url}"
            );
        }
    }

    #[test]
    fn render_report_title_does_not_expand_placeholders_inside_job_name() {
        let title = render_template_once(
            "Report {job} {date} {timestamp}",
            "daily {date}",
            "2026-05-14",
            "2026-05-14T09:00:00+08:00",
        );
        assert_eq!(
            title,
            "Report daily {date} 2026-05-14 2026-05-14T09:00:00+08:00"
        );
    }

    #[test]
    fn derive_report_summary_strips_nested_markdown_prefixes() {
        assert_eq!(derive_report_summary("  - ## Heading\nbody"), "Heading");
    }

    #[test]
    fn render_subject_substitutes_placeholder() {
        assert_eq!(render_subject(Some("Cron: {job}"), "daily"), "Cron: daily");
        assert_eq!(
            render_subject(Some("no placeholder"), "x"),
            "no placeholder"
        );
        assert_eq!(render_subject(None, "daily"), "Cron: daily");
        assert_eq!(render_subject(Some(""), "daily"), "Cron: daily");
    }

    // -- Minimal HTTP mock ---------------------------------------------------

    struct CapturedRequest {
        request_line: String,
        headers: Vec<String>,
        body: String,
    }

    /// Spawn a tiny TCP server that serves exactly one request, parses the
    /// HTTP/1.1 request line + headers + body, then responds with the given
    /// status code and response body. Returns `(port, oneshot_rx)` where the
    /// oneshot resolves once the request has been received.
    async fn spawn_mock_http_server(
        status: u16,
        response_body: &'static str,
    ) -> (u16, tokio::sync::oneshot::Receiver<CapturedRequest>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let (mut stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };

            // Read until we have full headers and the declared body.
            let mut buf = Vec::with_capacity(4096);
            let mut tmp = [0u8; 1024];
            let mut headers_end = None;
            let mut content_length: Option<usize> = None;
            loop {
                let n = match stream.read(&mut tmp).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => return,
                };
                buf.extend_from_slice(&tmp[..n]);
                if headers_end.is_none() {
                    if let Some(pos) = find_subsequence(&buf, b"\r\n\r\n") {
                        headers_end = Some(pos + 4);
                        // Parse Content-Length.
                        let head_str = String::from_utf8_lossy(&buf[..pos]);
                        for line in head_str.lines() {
                            if let Some(v) = line.strip_prefix("Content-Length: ") {
                                content_length = v.trim().parse::<usize>().ok();
                            } else if let Some(v) = line.strip_prefix("content-length: ") {
                                content_length = v.trim().parse::<usize>().ok();
                            }
                        }
                    }
                }
                if let (Some(end), Some(cl)) = (headers_end, content_length) {
                    if buf.len() >= end + cl {
                        break;
                    }
                }
                if headers_end.is_some() && content_length.is_none() {
                    break;
                }
            }

            // Split into headers + body.
            let head_end = headers_end.unwrap_or(buf.len());
            let head_str = String::from_utf8_lossy(&buf[..head_end.saturating_sub(4)]).to_string();
            let body_bytes = if head_end < buf.len() {
                &buf[head_end..]
            } else {
                &[][..]
            };
            let body = String::from_utf8_lossy(body_bytes).to_string();
            let mut head_lines = head_str.lines();
            let request_line = head_lines.next().unwrap_or_default().to_string();
            let headers: Vec<String> = head_lines.map(|l| l.to_string()).collect();

            // Send response.
            let status_text = reason_phrase_for_status(status);
            let response_body = response_body.as_bytes();
            let response = format!(
                "HTTP/1.1 {status} {status_text}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.write_all(response_body).await;
            let _ = stream.flush().await;

            let _ = tx.send(CapturedRequest {
                request_line,
                headers,
                body,
            });
        });

        (port, rx)
    }

    fn reason_phrase_for_status(status: u16) -> &'static str {
        match status {
            200 => "OK",
            201 => "Created",
            400 => "Bad Request",
            401 => "Unauthorized",
            404 => "Not Found",
            500 => "Internal Server Error",
            _ => panic!("unsupported mock HTTP status {status}"),
        }
    }

    fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }
}
