//! Email channel adapter (IMAP + SMTP).
//!
//! Polls IMAP for new emails and sends responses via SMTP using `lettre`.
//! Uses the subject line for agent routing (e.g., "\[coder\] Fix this bug").

use crate::types::{ChannelAdapter, ChannelContent, ChannelMessage, ChannelType, ChannelUser};
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use futures::Stream;
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::AsyncSmtpTransport;
use lettre::AsyncTransport;
use lettre::Tokio1Executor;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
#[cfg(windows)]
use std::io::ErrorKind;
use std::io::Write;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::raw::c_int;
use std::path::{Path, PathBuf};
use std::pin::Pin;
#[cfg(windows)]
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};
use zeroize::Zeroizing;

const EMAIL_FETCH_BATCH_SIZE: usize = 50;

/// SASL PLAIN authenticator for IMAP servers that reject LOGIN
/// (e.g., Lark/Larksuite which only advertise AUTH=PLAIN).
struct PlainAuthenticator {
    username: String,
    password: String,
}

impl imap::Authenticator for PlainAuthenticator {
    type Response = String;
    fn process(&self, _data: &[u8]) -> Self::Response {
        // SASL PLAIN: \0<username>\0<password>
        format!("\x00{}\x00{}", self.username, self.password)
    }
}

/// Reply context for email threading (In-Reply-To / Subject continuity).
#[derive(Debug, Clone)]
struct ReplyCtx {
    subject: String,
    message_id: String,
}

#[derive(Debug, Clone)]
struct FetchedEmail {
    folder: String,
    uid: u32,
    uid_validity: u32,
    from_addr: String,
    subject: String,
    message_id: String,
    body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct EmailCursorState {
    #[serde(default = "email_cursor_state_version")]
    version: u32,
    #[serde(default)]
    folders: HashMap<String, EmailFolderCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct EmailFolderCursor {
    uid_validity: u32,
    last_seen_uid: u32,
}

struct EmailStateLock {
    file: File,
    #[cfg(not(unix))]
    path: PathBuf,
}

#[cfg(unix)]
unsafe extern "C" {
    fn flock(fd: c_int, operation: c_int) -> c_int;
}

#[cfg(unix)]
const LOCK_EX: c_int = 2;
#[cfg(unix)]
const LOCK_NB: c_int = 4;
#[cfg(unix)]
const LOCK_UN: c_int = 8;

impl Drop for EmailStateLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            // SAFETY: flock only uses the owned file descriptor and does not
            // outlive this guard. Failure on drop is non-recoverable.
            let _ = unsafe { flock(self.file.as_raw_fd(), LOCK_UN) };
        }
        #[cfg(not(unix))]
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn email_cursor_state_version() -> u32 {
    1
}

/// Email channel adapter using IMAP for receiving and SMTP for sending.
pub struct EmailAdapter {
    /// IMAP server host.
    imap_host: String,
    /// IMAP port (993 for TLS).
    imap_port: u16,
    /// SMTP server host.
    smtp_host: String,
    /// SMTP port (587 for STARTTLS, 465 for implicit TLS).
    smtp_port: u16,
    /// Email address (used for both IMAP and SMTP).
    username: String,
    /// SECURITY: Password is zeroized on drop.
    password: Zeroizing<String>,
    /// How often to check for new emails.
    poll_interval: Duration,
    /// Which IMAP folders to monitor.
    folders: Vec<String>,
    /// Only process emails from these senders (empty = all).
    allowed_senders: Vec<String>,
    /// Durable cursor state path. The adapter advances this only after a
    /// fetched message has been accepted by the local channel pipeline.
    state_path: PathBuf,
    /// Per-account process lock. This prevents two OpenFang processes from
    /// polling the same mailbox state file and moving the cursor backward.
    _state_lock: EmailStateLock,
    /// Shutdown signal.
    shutdown_tx: Arc<watch::Sender<bool>>,
    shutdown_rx: watch::Receiver<bool>,
    /// Tracks reply context per sender for email threading.
    reply_ctx: Arc<DashMap<String, ReplyCtx>>,
}

impl EmailAdapter {
    /// Create a new email adapter.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        imap_host: String,
        imap_port: u16,
        smtp_host: String,
        smtp_port: u16,
        username: String,
        password: String,
        poll_interval_secs: u64,
        folders: Vec<String>,
        allowed_senders: Vec<String>,
        state_dir: PathBuf,
    ) -> Self {
        Self::try_new(
            imap_host,
            imap_port,
            smtp_host,
            smtp_port,
            username,
            password,
            poll_interval_secs,
            folders,
            allowed_senders,
            state_dir,
        )
        .expect("email adapter state lock acquisition failed")
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        imap_host: String,
        imap_port: u16,
        smtp_host: String,
        smtp_port: u16,
        username: String,
        password: String,
        poll_interval_secs: u64,
        folders: Vec<String>,
        allowed_senders: Vec<String>,
        state_dir: PathBuf,
    ) -> Result<Self, String> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let state_lock = acquire_email_state_lock(&state_dir, &username)?;
        let state_path = email_state_path(&state_dir, &username);
        Ok(Self {
            imap_host,
            imap_port,
            smtp_host,
            smtp_port,
            username,
            password: Zeroizing::new(password),
            poll_interval: Duration::from_secs(poll_interval_secs),
            folders: if folders.is_empty() {
                vec!["INBOX".to_string()]
            } else {
                folders
            },
            allowed_senders,
            state_path,
            _state_lock: state_lock,
            shutdown_tx: Arc::new(shutdown_tx),
            shutdown_rx,
            reply_ctx: Arc::new(DashMap::new()),
        })
    }

    /// Check if a sender is in the allowlist (empty = allow all). Used in tests.
    #[allow(dead_code)]
    fn is_allowed_sender(&self, sender: &str) -> bool {
        self.allowed_senders.is_empty() || self.allowed_senders.iter().any(|s| sender.contains(s))
    }

    /// Extract agent name from subject line brackets, e.g., "[coder] Fix the bug" -> Some("coder")
    fn extract_agent_from_subject(subject: &str) -> Option<String> {
        let subject = subject.trim();
        if subject.starts_with('[') {
            if let Some(end) = subject.find(']') {
                let agent = &subject[1..end];
                if !agent.is_empty() {
                    return Some(agent.to_string());
                }
            }
        }
        None
    }

    /// Strip the agent tag from a subject line.
    fn strip_agent_tag(subject: &str) -> String {
        let subject = subject.trim();
        if subject.starts_with('[') {
            if let Some(end) = subject.find(']') {
                return subject[end + 1..].trim().to_string();
            }
        }
        subject.to_string()
    }

    /// Build an async SMTP transport for sending emails.
    async fn build_smtp_transport(
        &self,
    ) -> Result<AsyncSmtpTransport<Tokio1Executor>, Box<dyn std::error::Error>> {
        let creds = Credentials::new(self.username.clone(), self.password.as_str().to_string());

        let transport = if self.smtp_port == 465 {
            // Implicit TLS (port 465)
            AsyncSmtpTransport::<Tokio1Executor>::relay(&self.smtp_host)?
                .port(self.smtp_port)
                .credentials(creds)
                .build()
        } else {
            // STARTTLS (port 587 or other)
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&self.smtp_host)?
                .port(self.smtp_port)
                .credentials(creds)
                .build()
        };

        Ok(transport)
    }
}

/// Extract `user@domain` from a potentially formatted email string like `"Name <user@domain>"`.
fn extract_email_addr(raw: &str) -> String {
    let raw = raw.trim();
    if let Some(start) = raw.find('<') {
        if let Some(end) = raw.find('>') {
            if end > start {
                return raw[start + 1..end].trim().to_string();
            }
        }
    }
    raw.to_string()
}

/// Get a specific header value from a parsed email.
fn get_header(parsed: &mailparse::ParsedMail<'_>, name: &str) -> Option<String> {
    parsed
        .headers
        .iter()
        .find(|h| h.get_key().eq_ignore_ascii_case(name))
        .map(|h| h.get_value())
}

/// Extract the text/plain body from a parsed email (handles multipart).
fn extract_text_body(parsed: &mailparse::ParsedMail<'_>) -> String {
    if parsed.subparts.is_empty() {
        return parsed.get_body().unwrap_or_default();
    }
    // Walk subparts looking for text/plain
    for part in &parsed.subparts {
        let ct = part.ctype.mimetype.to_lowercase();
        if ct == "text/plain" {
            return part.get_body().unwrap_or_default();
        }
    }
    // Fallback: first subpart body
    parsed
        .subparts
        .first()
        .and_then(|p| p.get_body().ok())
        .unwrap_or_default()
}

fn sanitize_state_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "email-account".to_string()
    } else {
        sanitized
    }
}

fn email_state_path(state_dir: &Path, username: &str) -> PathBuf {
    let digest = Sha256::digest(username.as_bytes());
    let suffix = format!("{:x}", digest);
    state_dir.join(format!(
        "{}-{}.json",
        sanitize_state_component(username),
        &suffix[..8]
    ))
}

fn email_lock_path(state_dir: &Path, username: &str) -> PathBuf {
    email_state_path(state_dir, username).with_extension("lock")
}

fn acquire_email_state_lock(state_dir: &Path, username: &str) -> Result<EmailStateLock, String> {
    fs::create_dir_all(state_dir).map_err(|e| {
        format!(
            "Failed to create email cursor state directory {}: {e}",
            state_dir.display()
        )
    })?;
    let lock_path = email_lock_path(state_dir, username);
    let mut file = open_email_lock_file(&lock_path)?;
    try_lock_email_state_file(&file, &lock_path)?;
    file.set_len(0).map_err(|e| {
        format!(
            "Failed to truncate email cursor lock {}: {e}",
            lock_path.display()
        )
    })?;
    writeln!(file, "pid={}", std::process::id()).map_err(|e| {
        format!(
            "Failed to write email cursor lock {}: {e}",
            lock_path.display()
        )
    })?;
    Ok(EmailStateLock {
        file,
        #[cfg(not(unix))]
        path: lock_path,
    })
}

#[cfg(unix)]
fn open_email_lock_file(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("Failed to open email cursor lock {}: {e}", path.display()))
}

#[cfg(windows)]
fn open_email_lock_file(path: &Path) -> Result<File, String> {
    match OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            if remove_stale_windows_email_lock(path)? {
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(path)
                    .map_err(|retry_error| {
                        format!(
                            "Email cursor state lock {} was stale but could not be reacquired: {retry_error}",
                            path.display()
                        )
                    })
            } else {
                Err(format!(
                    "Email cursor state is already locked by another OpenFang process: {}",
                    path.display()
                ))
            }
        }
        Err(error) => Err(format!(
            "Email cursor state is already locked or cannot be locked at {}: {error}",
            path.display()
        )),
    }
}

#[cfg(windows)]
fn remove_stale_windows_email_lock(path: &Path) -> Result<bool, String> {
    let raw = fs::read_to_string(path).unwrap_or_default();
    let Some(pid) = parse_email_lock_pid(&raw) else {
        return Ok(false);
    };
    if windows_pid_is_running(pid) {
        return Ok(false);
    }
    fs::remove_file(path)
        .map(|_| true)
        .map_err(|error| format!("Failed to remove stale email cursor lock {}: {error}", path.display()))
}

#[cfg(windows)]
fn parse_email_lock_pid(raw: &str) -> Option<u32> {
    raw.lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|pid| *pid > 0)
}

#[cfg(windows)]
fn windows_pid_is_running(pid: u32) -> bool {
    let Ok(output) = Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output()
    else {
        return true;
    };
    if !output.status.success() {
        return true;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.contains(&format!(",\"{pid}\","))
}

#[cfg(all(not(unix), not(windows)))]
fn open_email_lock_file(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| {
            format!(
                "Email cursor state is already locked or cannot be locked at {}: {e}",
                path.display()
            )
        })
}

#[cfg(unix)]
fn try_lock_email_state_file(file: &File, path: &Path) -> Result<(), String> {
    // SAFETY: flock is called with a valid, live file descriptor owned by the
    // guard. LOCK_NB makes the startup failure explicit instead of blocking.
    let result = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(format!(
            "Email cursor state is already locked by another OpenFang process: {}",
            path.display()
        ))
    }
}

#[cfg(not(unix))]
fn try_lock_email_state_file(_file: &File, _path: &Path) -> Result<(), String> {
    Ok(())
}

fn load_email_cursor_state(path: &Path) -> Result<EmailCursorState, String> {
    if !path.exists() {
        return Ok(EmailCursorState {
            version: email_cursor_state_version(),
            folders: HashMap::new(),
        });
    }
    let raw = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read email cursor state {}: {e}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|e| format!("Failed to parse email cursor state {}: {e}", path.display()))
}

fn save_email_cursor_state(path: &Path, state: &EmailCursorState) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "Failed to create email cursor state directory {}: {e}",
                parent.display()
            )
        })?;
    }
    let payload = serde_json::to_vec_pretty(state)
        .map_err(|e| format!("Failed to serialize email cursor state: {e}"))?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, payload).map_err(|e| {
        format!(
            "Failed to write email cursor state temp file {}: {e}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path).map_err(|e| {
        format!(
            "Failed to replace email cursor state {} with {}: {e}",
            path.display(),
            tmp_path.display()
        )
    })
}

#[cfg(test)]
fn record_email_cursor_success(
    path: &Path,
    folder: &str,
    uid_validity: u32,
    uid: u32,
) -> Result<(), String> {
    let mut state = load_email_cursor_state(path)?;
    advance_email_cursor(&mut state, folder, uid_validity, uid);
    save_email_cursor_state(path, &state)
}

fn advance_email_cursor(state: &mut EmailCursorState, folder: &str, uid_validity: u32, uid: u32) {
    let cursor = state.folders.entry(folder.to_string()).or_default();
    if cursor.uid_validity != uid_validity {
        cursor.uid_validity = uid_validity;
        cursor.last_seen_uid = uid;
    } else {
        cursor.last_seen_uid = cursor.last_seen_uid.max(uid);
    }
    state.version = email_cursor_state_version();
}

fn initialize_email_folder_cursor(
    path: &Path,
    folder: &str,
    uid_validity: u32,
    uid_next: u32,
) -> Result<(), String> {
    let last_seen_uid = uid_next.saturating_sub(1);
    let mut state = load_email_cursor_state(path)?;
    state.folders.insert(
        folder.to_string(),
        EmailFolderCursor {
            uid_validity,
            last_seen_uid,
        },
    );
    state.version = email_cursor_state_version();
    save_email_cursor_state(path, &state)
}

fn cursor_for_folder(
    state: &EmailCursorState,
    folder: &str,
    uid_validity: u32,
) -> Option<EmailFolderCursor> {
    state
        .folders
        .get(folder)
        .filter(|cursor| cursor.uid_validity == uid_validity)
        .cloned()
}

/// Fetch emails that have UIDs above the durable cursor.
///
/// On the first run for a folder, the adapter establishes a baseline at the
/// server's current UIDNEXT and does not backfill older messages. That prevents
/// a newly configured mailbox from flooding the channel with historical mail.
/// After the baseline exists, shutdown gaps are recovered by UID cursor.
fn fetch_new_emails(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    folders: &[String],
    state_path: &Path,
) -> Result<Vec<FetchedEmail>, String> {
    let client = imap::ClientBuilder::new(host, port)
        .connect()
        .map_err(|e| format!("IMAP connect failed: {e}"))?;

    // Try LOGIN first; fall back to AUTHENTICATE PLAIN for servers like Lark
    // that reject LOGIN and only support AUTH=PLAIN (SASL).
    let mut session = match client.login(username, password) {
        Ok(s) => s,
        Err((login_err, client)) => {
            let authenticator = PlainAuthenticator {
                username: username.to_string(),
                password: password.to_string(),
            };
            client
                .authenticate("PLAIN", &authenticator)
                .map_err(|(e, _)| {
                    format!("IMAP login failed: {login_err}; AUTH=PLAIN also failed: {e}")
                })?
        }
    };

    let state = load_email_cursor_state(state_path)?;
    let mut results = Vec::new();

    for folder in folders {
        let mailbox = match session.select(folder) {
            Ok(mailbox) => mailbox,
            Err(e) => {
                warn!(folder, error = %e, "IMAP SELECT failed, skipping folder");
                continue;
            }
        };

        let uid_validity = mailbox.uid_validity.unwrap_or(0);
        let uid_next = mailbox.uid_next.unwrap_or(1);
        let Some(cursor) = cursor_for_folder(&state, folder, uid_validity) else {
            initialize_email_folder_cursor(state_path, folder, uid_validity, uid_next)?;
            info!(
                folder,
                uid_validity,
                last_seen_uid = uid_next.saturating_sub(1),
                "Initialized email cursor baseline"
            );
            continue;
        };

        let start_uid = cursor.last_seen_uid.saturating_add(1).max(1);
        let search_query = format!("UID {start_uid}:*");
        let uids = match session.uid_search(search_query) {
            Ok(uids) => uids,
            Err(e) => {
                warn!(folder, error = %e, "IMAP UID SEARCH failed");
                continue;
            }
        };

        let mut uid_list: Vec<u32> = uids
            .into_iter()
            .filter(|uid| *uid > cursor.last_seen_uid)
            .collect();
        uid_list.sort_unstable();
        uid_list.truncate(EMAIL_FETCH_BATCH_SIZE);

        if uid_list.is_empty() {
            debug!(folder, "No new emails above cursor");
            continue;
        }

        let uid_set: String = uid_list
            .iter()
            .map(|u| u.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let fetches = match session.uid_fetch(&uid_set, "(UID RFC822)") {
            Ok(f) => f,
            Err(e) => {
                warn!(folder, error = %e, "IMAP FETCH failed");
                continue;
            }
        };

        for fetch in fetches.iter() {
            let Some(uid) = fetch.uid else {
                warn!(folder, "IMAP FETCH response missing UID, skipping email");
                continue;
            };
            let body_bytes = match fetch.body() {
                Some(b) => b,
                None => continue,
            };

            let parsed = match mailparse::parse_mail(body_bytes) {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "Failed to parse email");
                    continue;
                }
            };

            let from = get_header(&parsed, "From").unwrap_or_default();
            let subject = get_header(&parsed, "Subject").unwrap_or_default();
            let message_id = get_header(&parsed, "Message-ID").unwrap_or_default();
            let text_body = extract_text_body(&parsed);

            let from_addr = extract_email_addr(&from);
            results.push(FetchedEmail {
                folder: folder.clone(),
                uid,
                uid_validity,
                from_addr,
                subject,
                message_id,
                body: text_body,
            });
        }
    }

    let _ = session.logout();
    Ok(results)
}

#[async_trait]
impl ChannelAdapter for EmailAdapter {
    fn name(&self) -> &str {
        "email"
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::Email
    }

    async fn start(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ChannelMessage> + Send>>, Box<dyn std::error::Error>>
    {
        let (tx, rx) = mpsc::channel::<ChannelMessage>(256);
        let poll_interval = self.poll_interval;
        let imap_host = self.imap_host.clone();
        let imap_port = self.imap_port;
        let username = self.username.clone();
        let password = self.password.clone();
        let folders = self.folders.clone();
        let allowed_senders = self.allowed_senders.clone();
        let state_path = self.state_path.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let reply_ctx = self.reply_ctx.clone();

        info!(
            state_path = %state_path.display(),
            "Starting email adapter (IMAP: {}:{}, SMTP: {}:{}, polling every {:?})",
            imap_host, imap_port, self.smtp_host, self.smtp_port, poll_interval
        );

        tokio::spawn(async move {
            loop {
                loop {
                    // IMAP operations are blocking I/O — run in spawn_blocking.
                    let host = imap_host.clone();
                    let port = imap_port;
                    let user = username.clone();
                    let pass = password.clone();
                    let fldrs = folders.clone();
                    let path = state_path.clone();

                    let emails = tokio::task::spawn_blocking(move || {
                        fetch_new_emails(&host, port, &user, pass.as_str(), &fldrs, &path)
                    })
                    .await;

                    let emails = match emails {
                        Ok(Ok(emails)) => emails,
                        Ok(Err(e)) => {
                            error!("IMAP poll error: {e}");
                            break;
                        }
                        Err(e) => {
                            error!("IMAP spawn_blocking panic: {e}");
                            break;
                        }
                    };

                    if emails.is_empty() {
                        break;
                    }

                    let mut cursor_persist_failed = false;
                    let mut cursor_state = match load_email_cursor_state(&state_path) {
                        Ok(state) => state,
                        Err(e) => {
                            error!(
                                "Failed to load email cursor state before processing batch: {e}"
                            );
                            break;
                        }
                    };
                    let mut cursor_dirty = false;
                    for email in emails {
                        let FetchedEmail {
                            folder,
                            uid,
                            uid_validity,
                            from_addr,
                            subject,
                            message_id,
                            body,
                        } = email;

                        // Check allowed senders. A policy skip still advances
                        // the cursor so the adapter does not loop forever.
                        if !allowed_senders.is_empty()
                            && !allowed_senders.iter().any(|s| from_addr.contains(s))
                        {
                            debug!(from = %from_addr, uid, folder, "Email from non-allowed sender, skipping");
                            advance_email_cursor(&mut cursor_state, &folder, uid_validity, uid);
                            cursor_dirty = true;
                            continue;
                        }

                        // Store reply context for threading
                        if !message_id.is_empty() {
                            reply_ctx.insert(
                                from_addr.clone(),
                                ReplyCtx {
                                    subject: subject.clone(),
                                    message_id: message_id.clone(),
                                },
                            );
                        }

                        // Extract target agent from subject brackets (stored in metadata for router)
                        let _target_agent = EmailAdapter::extract_agent_from_subject(&subject);
                        let clean_subject = EmailAdapter::strip_agent_tag(&subject);

                        // Build the message body: prepend subject context
                        let text = if clean_subject.is_empty() {
                            body.trim().to_string()
                        } else {
                            format!("Subject: {clean_subject}\n\n{}", body.trim())
                        };

                        let account_scope = format!("account:{username}");
                        let mut metadata = std::collections::HashMap::new();
                        metadata.insert(
                            "account_id".to_string(),
                            serde_json::Value::String(username.clone()),
                        );
                        metadata.insert(
                            "channel_id".to_string(),
                            serde_json::Value::String(account_scope),
                        );
                        metadata.insert(
                            "sender_email".to_string(),
                            serde_json::Value::String(from_addr.clone()),
                        );
                        metadata.insert(
                            "subject".to_string(),
                            serde_json::Value::String(clean_subject.clone()),
                        );
                        metadata.insert(
                            "imap_folder".to_string(),
                            serde_json::Value::String(folder.clone()),
                        );
                        metadata.insert(
                            "imap_uid".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(uid)),
                        );
                        metadata.insert(
                            "imap_uid_validity".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(uid_validity)),
                        );

                        let msg = ChannelMessage {
                            channel: ChannelType::Email,
                            platform_message_id: if message_id.is_empty() {
                                format!("{folder}:{uid_validity}:{uid}")
                            } else {
                                message_id.clone()
                            },
                            sender: ChannelUser {
                                platform_id: from_addr.clone(),
                                display_name: from_addr.clone(),
                                openfang_user: None,
                            },
                            content: ChannelContent::Text(text),
                            target_agent: None, // Routing handled by bridge AgentRouter
                            timestamp: Utc::now(),
                            is_group: false,
                            thread_id: None,
                            metadata,
                        };

                        if tx.send(msg).await.is_err() {
                            info!("Email channel receiver dropped, stopping poll");
                            if cursor_dirty {
                                if let Err(e) = save_email_cursor_state(&state_path, &cursor_state)
                                {
                                    error!(
                                        "Failed to persist email cursor before stopping poll: {e}"
                                    );
                                }
                            }
                            return;
                        }

                        advance_email_cursor(&mut cursor_state, &folder, uid_validity, uid);
                        cursor_dirty = true;
                    }

                    if cursor_dirty {
                        if let Err(e) = save_email_cursor_state(&state_path, &cursor_state) {
                            error!("Failed to persist email cursor after batch: {e}");
                            cursor_persist_failed = true;
                        }
                    }

                    if cursor_persist_failed {
                        break;
                    }
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("Email adapter shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn send(
        &self,
        user: &ChannelUser,
        content: ChannelContent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match content {
            ChannelContent::Text(text) => {
                // Parse recipient address
                let to_addr = extract_email_addr(&user.platform_id);
                let to_mailbox: Mailbox = to_addr
                    .parse()
                    .map_err(|e| format!("Invalid recipient email '{}': {}", to_addr, e))?;

                let from_mailbox: Mailbox = self
                    .username
                    .parse()
                    .map_err(|e| format!("Invalid sender email '{}': {}", self.username, e))?;

                // Extract subject from text body convention: "Subject: ...\n\n..."
                let (subject, body) = if text.starts_with("Subject: ") {
                    if let Some(pos) = text.find("\n\n") {
                        let subj = text[9..pos].trim().to_string();
                        let body = text[pos + 2..].to_string();
                        (subj, body)
                    } else {
                        ("OpenFang Reply".to_string(), text)
                    }
                } else {
                    // Check reply context for subject continuity
                    let subj = self
                        .reply_ctx
                        .get(&to_addr)
                        .map(|ctx| format!("Re: {}", ctx.subject))
                        .unwrap_or_else(|| "OpenFang Reply".to_string());
                    (subj, text)
                };

                // Build email message
                let mut builder = lettre::Message::builder()
                    .from(from_mailbox)
                    .to(to_mailbox)
                    .subject(&subject);

                // Add In-Reply-To header for threading
                if let Some(ctx) = self.reply_ctx.get(&to_addr) {
                    if !ctx.message_id.is_empty() {
                        builder = builder.in_reply_to(ctx.message_id.clone());
                    }
                }

                let email = builder
                    .body(body)
                    .map_err(|e| format!("Failed to build email: {e}"))?;

                // Send via SMTP
                let transport = self.build_smtp_transport().await?;
                transport
                    .send(email)
                    .await
                    .map_err(|e| format!("SMTP send failed: {e}"))?;

                info!(
                    to = %to_addr,
                    subject = %subject,
                    "Email sent successfully via SMTP"
                );
            }
            _ => {
                warn!(
                    "Unsupported email content type for {}, only text is supported",
                    user.platform_id
                );
            }
        }
        Ok(())
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_email_adapter_creation() {
        let state_dir = tempdir().unwrap();
        let adapter = EmailAdapter::new(
            "imap.gmail.com".to_string(),
            993,
            "smtp.gmail.com".to_string(),
            587,
            "user@gmail.com".to_string(),
            "password".to_string(),
            30,
            vec![],
            vec![],
            state_dir.path().to_path_buf(),
        );
        assert_eq!(adapter.name(), "email");
        assert_eq!(adapter.folders, vec!["INBOX".to_string()]);
        assert_eq!(
            adapter.state_path,
            state_dir.path().join("user_gmail.com-02ee7bdc.json")
        );
    }

    #[test]
    fn test_email_state_path_includes_hash_to_avoid_alias_collisions() {
        let state_dir = tempdir().unwrap();
        let first = email_state_path(state_dir.path(), "bob@foo.com");
        let second = email_state_path(state_dir.path(), "bob+foo.com");

        assert_ne!(first, second);
        assert!(first
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("bob_foo.com-"));
        assert!(second
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("bob_foo.com-"));
    }

    #[test]
    fn test_allowed_senders() {
        let state_dir = tempdir().unwrap();
        let adapter = EmailAdapter::new(
            "imap.example.com".to_string(),
            993,
            "smtp.example.com".to_string(),
            587,
            "bot@example.com".to_string(),
            "pass".to_string(),
            30,
            vec![],
            vec!["boss@company.com".to_string()],
            state_dir.path().to_path_buf(),
        );
        assert!(adapter.is_allowed_sender("boss@company.com"));
        assert!(!adapter.is_allowed_sender("random@other.com"));
        drop(adapter);

        let open = EmailAdapter::new(
            "imap.example.com".to_string(),
            993,
            "smtp.example.com".to_string(),
            587,
            "bot@example.com".to_string(),
            "pass".to_string(),
            30,
            vec![],
            vec![],
            state_dir.path().to_path_buf(),
        );
        assert!(open.is_allowed_sender("anyone@anywhere.com"));
    }

    #[test]
    fn test_email_state_lock_rejects_second_process_scope() {
        let state_dir = tempdir().unwrap();
        let _first = EmailAdapter::new(
            "imap.example.com".to_string(),
            993,
            "smtp.example.com".to_string(),
            587,
            "bot@example.com".to_string(),
            "pass".to_string(),
            30,
            vec![],
            vec![],
            state_dir.path().to_path_buf(),
        );

        let second = EmailAdapter::try_new(
            "imap.example.com".to_string(),
            993,
            "smtp.example.com".to_string(),
            587,
            "bot@example.com".to_string(),
            "pass".to_string(),
            30,
            vec![],
            vec![],
            state_dir.path().to_path_buf(),
        );

        assert!(second.is_err());
    }

    #[test]
    fn test_email_cursor_success_is_monotonic() {
        let state_dir = tempdir().unwrap();
        let path = email_state_path(state_dir.path(), "typingpawmi@gmail.com");

        record_email_cursor_success(&path, "INBOX", 42, 10).unwrap();
        record_email_cursor_success(&path, "INBOX", 42, 8).unwrap();

        let state = load_email_cursor_state(&path).unwrap();
        assert_eq!(
            state.folders.get("INBOX"),
            Some(&EmailFolderCursor {
                uid_validity: 42,
                last_seen_uid: 10
            })
        );
    }

    #[test]
    fn test_email_cursor_uidvalidity_reset() {
        let state_dir = tempdir().unwrap();
        let path = email_state_path(state_dir.path(), "typingpawmi@gmail.com");

        record_email_cursor_success(&path, "INBOX", 42, 10).unwrap();
        record_email_cursor_success(&path, "INBOX", 43, 3).unwrap();

        let state = load_email_cursor_state(&path).unwrap();
        assert_eq!(
            state.folders.get("INBOX"),
            Some(&EmailFolderCursor {
                uid_validity: 43,
                last_seen_uid: 3
            })
        );
    }

    #[test]
    fn test_initialize_email_folder_cursor_sets_current_baseline() {
        let state_dir = tempdir().unwrap();
        let path = email_state_path(state_dir.path(), "typingpawmi@gmail.com");

        initialize_email_folder_cursor(&path, "INBOX", 42, 101).unwrap();

        let state = load_email_cursor_state(&path).unwrap();
        assert_eq!(
            state.folders.get("INBOX"),
            Some(&EmailFolderCursor {
                uid_validity: 42,
                last_seen_uid: 100
            })
        );
    }

    #[test]
    fn test_email_account_scope_metadata_shape() {
        let username = "typingpawmi@gmail.com".to_string();
        let from_addr = "client@example.com".to_string();
        let clean_subject = "Project update".to_string();
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            "account_id".to_string(),
            serde_json::Value::String(username.clone()),
        );
        metadata.insert(
            "channel_id".to_string(),
            serde_json::Value::String(format!("account:{username}")),
        );
        metadata.insert(
            "sender_email".to_string(),
            serde_json::Value::String(from_addr.clone()),
        );
        metadata.insert(
            "subject".to_string(),
            serde_json::Value::String(clean_subject.clone()),
        );
        let message = ChannelMessage {
            channel: ChannelType::Email,
            platform_message_id: "message-id".to_string(),
            sender: ChannelUser {
                platform_id: from_addr,
                display_name: "client@example.com".to_string(),
                openfang_user: None,
            },
            content: ChannelContent::Text("Subject: Project update\n\nBody".to_string()),
            target_agent: None,
            timestamp: Utc::now(),
            is_group: false,
            thread_id: None,
            metadata,
        };

        assert_eq!(
            message.channel_id().as_deref(),
            Some("account:typingpawmi@gmail.com")
        );
        assert_eq!(
            message
                .metadata
                .get("account_id")
                .and_then(|value| value.as_str()),
            Some("typingpawmi@gmail.com")
        );
    }

    #[test]
    fn test_extract_agent_from_subject() {
        assert_eq!(
            EmailAdapter::extract_agent_from_subject("[coder] Fix the bug"),
            Some("coder".to_string())
        );
        assert_eq!(
            EmailAdapter::extract_agent_from_subject("[researcher] Find papers on AI"),
            Some("researcher".to_string())
        );
        assert_eq!(
            EmailAdapter::extract_agent_from_subject("No brackets here"),
            None
        );
        assert_eq!(
            EmailAdapter::extract_agent_from_subject("[] Empty brackets"),
            None
        );
    }

    #[test]
    fn test_strip_agent_tag() {
        assert_eq!(
            EmailAdapter::strip_agent_tag("[coder] Fix the bug"),
            "Fix the bug"
        );
        assert_eq!(EmailAdapter::strip_agent_tag("No brackets"), "No brackets");
    }

    #[test]
    fn test_extract_email_addr() {
        assert_eq!(
            extract_email_addr("John Doe <john@example.com>"),
            "john@example.com"
        );
        assert_eq!(extract_email_addr("user@example.com"), "user@example.com");
        assert_eq!(extract_email_addr("<user@test.com>"), "user@test.com");
    }

    #[test]
    fn test_subject_extraction_from_body() {
        let text = "Subject: Test Subject\n\nThis is the body.";
        assert!(text.starts_with("Subject: "));
        let pos = text.find("\n\n").unwrap();
        let subject = &text[9..pos];
        let body = &text[pos + 2..];
        assert_eq!(subject, "Test Subject");
        assert_eq!(body, "This is the body.");
    }

    #[test]
    fn test_reply_ctx_threading() {
        let ctx_map: DashMap<String, ReplyCtx> = DashMap::new();
        ctx_map.insert(
            "user@test.com".to_string(),
            ReplyCtx {
                subject: "Original Subject".to_string(),
                message_id: "<msg-123@test.com>".to_string(),
            },
        );
        let ctx = ctx_map.get("user@test.com").unwrap();
        assert_eq!(ctx.subject, "Original Subject");
        assert_eq!(ctx.message_id, "<msg-123@test.com>");
    }
}
