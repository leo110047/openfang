//! ChatGPT/Codex subscription backend driver.
//!
//! This is intentionally separate from the `openai` and `codex` providers.
//! It uses the Codex CLI's ChatGPT OAuth token with the Codex product backend,
//! not the public OpenAI API.

use crate::llm_driver::{CompletionRequest, CompletionResponse, LlmDriver, LlmError, StreamEvent};
use async_trait::async_trait;
use base64::Engine;
use futures::StreamExt;
use openfang_types::message::{ContentBlock, MessageContent, Role, StopReason, TokenUsage};
use openfang_types::tool::ToolCall;
use serde::Serialize;
use serde_json::Value;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{trace, warn};
use zeroize::Zeroizing;

const DEFAULT_CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
const CODEX_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const CODEX_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

fn codex_stream_timeout_error() -> LlmError {
    LlmError::Http(format!(
        "Codex response stream timed out after {}s",
        CODEX_REQUEST_TIMEOUT.as_secs()
    ))
}

fn remaining_before_deadline(deadline: Instant) -> Result<Duration, LlmError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(codex_stream_timeout_error());
    }
    Ok(remaining)
}

/// Driver for the ChatGPT-hosted Codex Responses backend.
pub struct ChatGptCodexDriver {
    access_token: Zeroizing<String>,
    account_id: Option<String>,
    base_url: String,
    client: reqwest::Client,
}

impl ChatGptCodexDriver {
    /// Create a new ChatGPT/Codex driver from a Codex OAuth access token.
    pub fn new(access_token: String, base_url: Option<String>) -> Self {
        let account_id = chatgpt_account_id(&access_token);
        Self {
            access_token: Zeroizing::new(access_token),
            account_id,
            base_url: base_url.unwrap_or_else(|| DEFAULT_CODEX_BASE_URL.to_string()),
            client: reqwest::Client::builder()
                .user_agent("codex_cli_rs/0.0.0 (OpenFang)")
                .connect_timeout(CODEX_CONNECT_TIMEOUT)
                .timeout(CODEX_REQUEST_TIMEOUT)
                .build()
                .expect("Codex driver requires reqwest client"),
        }
    }

    fn responses_url(&self) -> String {
        format!("{}/responses", self.base_url.trim_end_matches('/'))
    }

    fn apply_headers(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        req = req
            .header(
                "authorization",
                format!("Bearer {}", self.access_token.as_str()),
            )
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("originator", "codex_cli_rs")
            .header("user-agent", "codex_cli_rs/0.0.0 (OpenFang)");
        if let Some(account_id) = &self.account_id {
            req = req.header("ChatGPT-Account-ID", account_id);
        }
        req
    }

    async fn complete_or_stream(
        &self,
        request: CompletionRequest,
        tx: Option<tokio::sync::mpsc::Sender<StreamEvent>>,
    ) -> Result<CompletionResponse, LlmError> {
        let deadline = Instant::now() + CODEX_REQUEST_TIMEOUT;
        let codex_request = CodexRequest::from_completion(request);
        let resp = self
            .apply_headers(self.client.post(self.responses_url()))
            .json(&codex_request)
            .send()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;

        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            if status == 401 || status == 403 {
                return Err(LlmError::AuthenticationFailed(body));
            }
            if status == 429 {
                return Err(LlmError::RateLimited {
                    retry_after_ms: 5000,
                });
            }
            return Err(LlmError::Api {
                status,
                message: body,
            });
        }

        let mut parser = SseParser::default();
        let mut text = String::new();
        let mut final_text: Option<String> = None;
        let mut tool_calls = Vec::new();
        let mut usage = TokenUsage::default();
        let mut stop_reason = StopReason::EndTurn;
        let mut error: Option<LlmError> = None;

        let mut stream = resp.bytes_stream();
        while let Some(chunk) =
            tokio::time::timeout(remaining_before_deadline(deadline)?, stream.next())
                .await
                .map_err(|_| codex_stream_timeout_error())?
        {
            let chunk = chunk.map_err(|e| LlmError::Http(e.to_string()))?;
            let events = parser.push(&String::from_utf8_lossy(&chunk));
            for event in events {
                let Some(data) = event.data else {
                    continue;
                };
                let value: Value = serde_json::from_str(&data)
                    .map_err(|e| LlmError::Parse(format!("Invalid SSE JSON: {e}")))?;
                match value.get("type").and_then(Value::as_str) {
                    Some("response.output_text.delta") => {
                        if let Some(delta) = value.get("delta").and_then(Value::as_str) {
                            text.push_str(delta);
                            if let Some(sender) = tx.as_ref() {
                                let _ = sender
                                    .send(StreamEvent::TextDelta {
                                        text: delta.to_string(),
                                    })
                                    .await;
                            }
                        }
                    }
                    Some("response.output_text.done") => {
                        final_text = value
                            .get("text")
                            .and_then(Value::as_str)
                            .map(ToString::to_string);
                    }
                    Some("response.output_item.done") => {
                        if let Some(item) = value.get("item") {
                            collect_output_item(item, &mut final_text, &mut tool_calls);
                        }
                    }
                    Some("response.completed") => {
                        if let Some(response_usage) =
                            value.get("response").and_then(|r| r.get("usage"))
                        {
                            usage = parse_usage(response_usage);
                        }
                    }
                    Some("response.incomplete") => {
                        stop_reason = StopReason::MaxTokens;
                    }
                    Some("response.failed") => {
                        let message = value
                            .get("response")
                            .and_then(|r| r.get("error"))
                            .map(Value::to_string)
                            .unwrap_or_else(|| data.clone());
                        error = Some(LlmError::Api {
                            status: 500,
                            message,
                        });
                    }
                    _ => {}
                }
            }
        }

        for event in parser.finish() {
            if let Some(data) = event.data {
                let value: Value = serde_json::from_str(&data)
                    .map_err(|e| LlmError::Parse(format!("Invalid trailing SSE JSON: {e}")))?;
                if value.get("type").and_then(Value::as_str) == Some("response.completed") {
                    if let Some(response_usage) = value.get("response").and_then(|r| r.get("usage"))
                    {
                        usage = parse_usage(response_usage);
                    }
                }
            }
        }

        if let Some(error) = error {
            return Err(error);
        }

        if let Some(done_text) = final_text {
            text = done_text;
        }
        if !tool_calls.is_empty() {
            stop_reason = StopReason::ToolUse;
        }
        if usage.input_tokens == 0
            && usage.output_tokens == 0
            && (!text.is_empty() || !tool_calls.is_empty())
        {
            // The ChatGPT Codex backend can omit usage. Keep a minimal non-zero
            // count so downstream metering and summaries know work occurred.
            usage.output_tokens = 1;
        }

        let mut content = Vec::new();
        if !text.is_empty() {
            content.push(ContentBlock::Text {
                text,
                provider_metadata: None,
            });
        }
        for call in &tool_calls {
            content.push(ContentBlock::ToolUse {
                id: call.id.clone(),
                name: call.name.clone(),
                input: call.input.clone(),
                provider_metadata: None,
            });
        }

        if let Some(sender) = tx.as_ref() {
            let _ = sender
                .send(StreamEvent::ContentComplete { stop_reason, usage })
                .await;
        }

        Ok(CompletionResponse {
            content,
            stop_reason,
            tool_calls,
            usage,
        })
    }
}

#[async_trait]
impl LlmDriver for ChatGptCodexDriver {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.complete_or_stream(request, None).await
    }

    async fn stream(
        &self,
        request: CompletionRequest,
        tx: tokio::sync::mpsc::Sender<StreamEvent>,
    ) -> Result<CompletionResponse, LlmError> {
        self.complete_or_stream(request, Some(tx)).await
    }
}

#[derive(Debug, Serialize)]
struct CodexRequest {
    model: String,
    instructions: String,
    input: Vec<Value>,
    stream: bool,
    store: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<Value>,
}

impl CodexRequest {
    fn from_completion(request: CompletionRequest) -> Self {
        let instructions = request
            .system
            .clone()
            .or_else(|| {
                request
                    .messages
                    .iter()
                    .find_map(|m| match (&m.role, &m.content) {
                        (Role::System, MessageContent::Text(text)) => Some(text.clone()),
                        _ => None,
                    })
            })
            .unwrap_or_else(|| "You are a helpful assistant.".to_string());

        let mut input = Vec::new();
        for msg in &request.messages {
            match (&msg.role, &msg.content) {
                (Role::System, _) => {}
                (Role::User, MessageContent::Text(text)) => {
                    input.push(message_item("user", "input_text", text));
                }
                (Role::Assistant, MessageContent::Text(text)) => {
                    if !text.trim().is_empty() {
                        input.push(message_item("assistant", "output_text", text));
                    }
                }
                (Role::User, MessageContent::Blocks(blocks)) => {
                    let mut user_parts = Vec::new();
                    for block in blocks {
                        match block {
                            ContentBlock::Text { text, .. } => {
                                user_parts.push(serde_json::json!({
                                    "type": "input_text",
                                    "text": text,
                                }));
                            }
                            ContentBlock::Image { media_type, data } => {
                                user_parts.push(serde_json::json!({
                                    "type": "input_image",
                                    "image_url": format!("data:{media_type};base64,{data}"),
                                }));
                            }
                            ContentBlock::ToolResult {
                                tool_use_id,
                                content,
                                ..
                            } => {
                                if let Some(call_id) = split_codex_tool_id(tool_use_id).0 {
                                    input.push(serde_json::json!({
                                        "type": "function_call_output",
                                        "call_id": call_id,
                                        "output": if content.is_empty() { "(empty)" } else { content },
                                    }));
                                }
                            }
                            _ => {}
                        }
                    }
                    if !user_parts.is_empty() {
                        input.push(serde_json::json!({
                            "role": "user",
                            "content": user_parts,
                        }));
                    }
                }
                (Role::Assistant, MessageContent::Blocks(blocks)) => {
                    let mut text_parts = Vec::new();
                    for block in blocks {
                        match block {
                            ContentBlock::Text { text, .. } if !text.trim().is_empty() => {
                                text_parts.push(serde_json::json!({
                                    "type": "output_text",
                                    "text": text,
                                }));
                            }
                            ContentBlock::ToolUse {
                                id,
                                name,
                                input: args,
                                ..
                            } => {
                                let (call_id, _) = split_codex_tool_id(id);
                                input.push(serde_json::json!({
                                    "type": "function_call",
                                    "call_id": call_id.unwrap_or_else(|| id.clone()),
                                    "name": name,
                                    "arguments": serde_json::to_string(args).unwrap_or_else(|_| "{}".to_string()),
                                }));
                            }
                            _ => {}
                        }
                    }
                    if !text_parts.is_empty() {
                        input.push(serde_json::json!({
                            "role": "assistant",
                            "content": text_parts,
                        }));
                    }
                }
            }
        }

        let tools: Vec<Value> = request
            .tools
            .iter()
            .map(|tool| {
                serde_json::json!({
                    "type": "function",
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": openfang_types::tool::normalize_schema_for_provider(
                        &tool.input_schema,
                        "openai",
                    ),
                    "strict": false,
                })
            })
            .collect();

        let (model, reasoning_effort) = parse_model_and_reasoning_effort(&request.model);

        Self {
            model,
            instructions,
            input,
            stream: true,
            store: false,
            reasoning: reasoning_effort.map(|effort| serde_json::json!({ "effort": effort })),
            // The ChatGPT-hosted Codex backend currently rejects
            // `max_output_tokens`; leave output budgeting to OpenFang's
            // agent/resource layer.
            max_output_tokens: None,
            tool_choice: if tools.is_empty() {
                None
            } else {
                Some(serde_json::json!("auto"))
            },
            tools,
        }
    }
}

fn parse_model_and_reasoning_effort(model: &str) -> (String, Option<String>) {
    let Some((base, effort)) = model.rsplit_once(':') else {
        return (model.to_string(), None);
    };
    match effort {
        "minimal" | "low" | "medium" | "high" | "xhigh" => {
            (base.to_string(), Some(effort.to_string()))
        }
        _ => (model.to_string(), None),
    }
}

fn message_item(role: &str, part_type: &str, text: &str) -> Value {
    serde_json::json!({
        "role": role,
        "content": [{
            "type": part_type,
            "text": text,
        }],
    })
}

fn collect_output_item(
    item: &Value,
    final_text: &mut Option<String>,
    tool_calls: &mut Vec<ToolCall>,
) {
    match item.get("type").and_then(Value::as_str) {
        Some("message") => {
            if let Some(parts) = item.get("content").and_then(Value::as_array) {
                let text = parts
                    .iter()
                    .filter_map(|part| {
                        let part_type = part.get("type").and_then(Value::as_str)?;
                        if part_type == "output_text" || part_type == "text" {
                            part.get("text").and_then(Value::as_str)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("");
                if !text.is_empty() {
                    *final_text = Some(text);
                }
            }
        }
        Some("function_call") => {
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                return;
            }
            let call_id = item
                .get("call_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if call_id.is_empty() {
                return;
            }
            let item_id = item.get("id").and_then(Value::as_str).unwrap_or("");
            let id = if item_id.is_empty() {
                call_id.clone()
            } else {
                format!("{call_id}|{item_id}")
            };
            let args = item
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or("{}");
            let input = match serde_json::from_str(args) {
                Ok(input) => input,
                Err(err) => {
                    warn!(
                        tool_name = %name,
                        call_id = %call_id,
                        error = %err,
                        raw_arguments = %args,
                        "Codex returned invalid JSON tool arguments"
                    );
                    serde_json::json!({
                        "__openfang_error": "invalid_json_arguments",
                        "__raw_arguments": args,
                    })
                }
            };
            tool_calls.push(ToolCall { id, name, input });
        }
        _ => {}
    }
}

fn parse_usage(usage: &Value) -> TokenUsage {
    TokenUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    }
}

fn split_codex_tool_id(raw: &str) -> (Option<String>, Option<String>) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return (None, None);
    }
    if let Some((call_id, response_item_id)) = trimmed.split_once('|') {
        return (
            nonempty(call_id).map(ToString::to_string),
            nonempty(response_item_id).map(ToString::to_string),
        );
    }
    if trimmed.starts_with("fc_") {
        return (None, Some(trimmed.to_string()));
    }
    (Some(trimmed.to_string()), None)
}

fn nonempty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

#[derive(Default)]
struct SseParser {
    buffer: String,
}

struct SseEvent {
    data: Option<String>,
}

impl SseParser {
    fn push(&mut self, chunk: &str) -> Vec<SseEvent> {
        self.buffer.push_str(chunk);
        let mut events = Vec::new();
        while let Some((idx, delimiter_len)) = find_sse_delimiter(&self.buffer) {
            let raw = self.buffer[..idx].to_string();
            self.buffer.drain(..idx + delimiter_len);
            events.push(parse_sse_event(&raw));
        }
        events
    }

    fn finish(&mut self) -> Vec<SseEvent> {
        if self.buffer.trim().is_empty() {
            self.buffer.clear();
            Vec::new()
        } else {
            let raw = std::mem::take(&mut self.buffer);
            vec![parse_sse_event(&raw)]
        }
    }
}

fn find_sse_delimiter(buffer: &str) -> Option<(usize, usize)> {
    match (buffer.find("\n\n"), buffer.find("\r\n\r\n")) {
        (Some(a), Some(b)) if a < b => Some((a, 2)),
        (Some(_), Some(b)) => Some((b, 4)),
        (Some(a), None) => Some((a, 2)),
        (None, Some(b)) => Some((b, 4)),
        (None, None) => None,
    }
}

fn parse_sse_event(raw: &str) -> SseEvent {
    let data = raw
        .lines()
        .filter_map(|line| line.trim_end_matches('\r').strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>()
        .join("\n");
    SseEvent {
        data: if data.is_empty() { None } else { Some(data) },
    }
}

/// Return true if a valid Codex CLI access token is present.
pub fn codex_access_token_available() -> bool {
    read_codex_access_token().is_some()
}

/// Read the Codex CLI ChatGPT OAuth access token.
pub fn read_codex_access_token() -> Option<String> {
    let auth_path = codex_auth_path()?;
    let content = std::fs::read_to_string(auth_path).ok()?;
    let parsed: Value = serde_json::from_str(&content).ok()?;
    let token = parsed
        .get("tokens")
        .and_then(|tokens| tokens.get("access_token"))
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())?;
    if jwt_is_expired(token) {
        return None;
    }
    Some(token.to_string())
}

fn codex_auth_path() -> Option<PathBuf> {
    let codex_home = std::env::var("CODEX_HOME")
        .map(PathBuf::from)
        .ok()
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|home| PathBuf::from(home).join(".codex"))
        })?;
    Some(codex_home.join("auth.json"))
}

fn jwt_is_expired(token: &str) -> bool {
    let Some(claims) = jwt_claims(token) else {
        trace!("Codex access token did not contain decodable JWT claims");
        return false;
    };
    let Some(exp) = claims.get("exp").and_then(Value::as_i64) else {
        trace!("Codex access token JWT has no numeric exp claim");
        return false;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    now >= exp
}

fn chatgpt_account_id(token: &str) -> Option<String> {
    jwt_claims(token)?
        .get("https://api.openai.com/auth")?
        .get("chatgpt_account_id")?
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .map(ToString::to_string)
}

fn jwt_claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let decoded = decode_base64_url(payload)?;
    serde_json::from_slice(&decoded).ok()
}

fn decode_base64_url(payload: &str) -> Option<Vec<u8>> {
    let mut normalized = payload.to_string();
    let padding = (4 - normalized.len() % 4) % 4;
    normalized.extend(std::iter::repeat_n('=', padding));
    base64::engine::general_purpose::URL_SAFE
        .decode(normalized)
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_parser_extracts_data() {
        let mut parser = SseParser::default();
        let events = parser.push("event: x\ndata: {\"type\":\"ok\"}\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.as_deref(), Some("{\"type\":\"ok\"}"));
    }

    #[test]
    fn split_codex_tool_id_round_trips_call_id() {
        assert_eq!(
            split_codex_tool_id("call_abc|fc_123"),
            (Some("call_abc".to_string()), Some("fc_123".to_string()))
        );
        assert_eq!(
            split_codex_tool_id("call_only").0.as_deref(),
            Some("call_only")
        );
    }

    #[test]
    fn jwt_claims_decodes_chatgpt_account_id() {
        let claims = serde_json::json!({
            "exp": 4_102_444_800i64,
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct_123"
            }
        });
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).unwrap());
        let token = format!("header.{encoded}.signature");

        assert!(!jwt_is_expired(&token));
        assert_eq!(chatgpt_account_id(&token).as_deref(), Some("acct_123"));
    }

    #[test]
    fn codex_http_timeouts_are_bounded() {
        assert_eq!(CODEX_CONNECT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(CODEX_REQUEST_TIMEOUT, Duration::from_secs(300));
    }

    #[test]
    fn codex_stream_deadline_reports_timeout() {
        let past_deadline = Instant::now() - Duration::from_secs(1);
        let err = remaining_before_deadline(past_deadline).unwrap_err();
        assert!(err.to_string().contains("Codex response stream timed out"));
    }

    #[test]
    fn invalid_tool_arguments_are_preserved() {
        let item = serde_json::json!({
            "type": "function_call",
            "name": "studio_os",
            "call_id": "call_bad_args",
            "arguments": "{\"action\":"
        });
        let mut final_text = None;
        let mut tool_calls = Vec::new();

        collect_output_item(&item, &mut final_text, &mut tool_calls);

        assert_eq!(tool_calls.len(), 1);
        assert_eq!(
            tool_calls[0].input["__openfang_error"],
            "invalid_json_arguments"
        );
        assert_eq!(tool_calls[0].input["__raw_arguments"], "{\"action\":");
    }
}
