use super::runner::{browser_config, lock_profile, navigate, profile_dir, run_js_json};
use super::security::validate_platform_url;
use super::types::DispatchOutput;
use super::{DispatchArgs, InspectArgs};
use openfang_runtime::browser::BrowserManager;
use openfang_types::outreach::OutreachPlatformManifest;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub(super) async fn verify_selectors(
    manifest: &OutreachPlatformManifest,
    args: &InspectArgs,
) -> Result<Value, String> {
    validate_platform_url(&args.source_url, manifest)?;
    let selectors = resolved_selectors(manifest)?;
    let _lock = lock_profile(manifest, args.profile_root.as_deref())?;
    let manager = BrowserManager::new(browser_config(manifest, args)?);
    let agent_id = format!("outreach-{}", manifest.key);
    navigate(&manager, &agent_id, &args.source_url).await?;
    let expression = format!(
        r#"(() => {{
const selectors = {};
const counts = Object.fromEntries(Object.entries(selectors).map(([k, v]) => [k, document.querySelectorAll(v).length]));
return JSON.stringify({{status: Object.values(counts).every((count) => count > 0) ? "ok" : "blocked", counts}});
}})()"#,
        serde_json::to_string(&selectors).unwrap_or_else(|_| "{}".to_string())
    );
    let value = run_js_json(&manager, &agent_id, &expression).await?;
    manager.close_session(&agent_id).await;
    Ok(value)
}

pub(super) async fn dispatch_message(
    manifest: &OutreachPlatformManifest,
    args: &DispatchArgs,
) -> Result<DispatchOutput, String> {
    validate_platform_url(&args.inspect.source_url, manifest)?;
    let message_body = dispatch_message_body(args)?;
    let message = message_body.trim();
    if message.is_empty() {
        return Ok(dispatch_blocked(
            manifest,
            "approved outreach message is empty",
            &args.inspect.source_url,
        ));
    }
    let expected_cost_label = match expected_cost_label_for_dispatch(manifest, args) {
        Ok(value) => value,
        Err(note) => {
            return Ok(dispatch_blocked(manifest, note, &args.inspect.source_url));
        }
    };
    let idempotency_path = dispatch_idempotency_path(manifest, args)?;
    if let Some(output) = read_cached_dispatch(&idempotency_path)? {
        return Ok(output);
    }
    let selectors = resolved_selectors(manifest)?;
    let _lock = lock_profile(manifest, args.inspect.profile_root.as_deref())?;
    let manager = BrowserManager::new(browser_config(manifest, &args.inspect)?);
    let agent_id = format!("outreach-{}", manifest.key);
    navigate(&manager, &agent_id, &args.inspect.source_url).await?;
    let cost_gate = verify_dispatch_cost(
        &manager,
        &agent_id,
        manifest,
        &args.inspect.source_url,
        expected_cost_label.as_deref(),
    )
    .await?;
    if let Some(blocked) = cost_gate.blocked {
        manager.close_session(&agent_id).await;
        return Ok(blocked);
    }
    let expression = format!(
        r#"(async () => {{
const selectors = {};
const message = {};
const visible = (node) => {{
  if (!node) return false;
  const rect = node.getBoundingClientRect();
  const style = window.getComputedStyle(node);
  return rect.width > 0 && rect.height > 0 && style.visibility !== "hidden" && style.display !== "none";
}};
const visibleMatches = (selector) => Array.from(document.querySelectorAll(selector)).filter(visible);
if (visibleMatches(selectors.success).length > 0) {{
  return JSON.stringify({{status: "blocked", reason: "success selector already visible before dispatch", url: location.href}});
}}
const messageEl = document.querySelector(selectors.message);
if (!messageEl) return JSON.stringify({{status: "blocked", reason: "message selector not found"}});
messageEl.focus();
messageEl.value = message;
messageEl.dispatchEvent(new Event("input", {{bubbles: true}}));
messageEl.dispatchEvent(new Event("change", {{bubbles: true}}));
const sendEl = document.querySelector(selectors.send);
if (!sendEl) return JSON.stringify({{status: "blocked", reason: "send selector not found"}});
sendEl.scrollIntoView({{block: "center"}});
sendEl.click();
const deadline = Date.now() + 15000;
while (Date.now() < deadline) {{
  if (visibleMatches(selectors.success).length > 0) {{
    return JSON.stringify({{status: "sent", url: location.href}});
  }}
  await new Promise((resolve) => setTimeout(resolve, 250));
}}
return JSON.stringify({{status: "blocked", reason: "success selector not observed", url: location.href}});
}})()"#,
        serde_json::to_string(&selectors).unwrap_or_else(|_| "{}".to_string()),
        serde_json::to_string(message).unwrap_or_else(|_| "\"\"".to_string())
    );
    let value = run_js_json(&manager, &agent_id, &expression).await?;
    manager.close_session(&agent_id).await;
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("failed")
        .to_string();
    let url = value
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or(&args.inspect.source_url)
        .to_string();
    if status == "sent" {
        let output = DispatchOutput {
            status,
            adapter: format!("openfang_{}", manifest.key),
            summary: format!("{} outreach message sent.", manifest.display_name),
            note: String::new(),
            destination: url.clone(),
            external_url: url,
            cost_snapshot: cost_gate.snapshot,
        };
        write_cached_dispatch(&idempotency_path, &output)?;
        Ok(output)
    } else {
        Ok(DispatchOutput {
            status: "blocked".to_string(),
            adapter: format!("openfang_{}", manifest.key),
            summary: format!("{} outreach dispatch blocked.", manifest.display_name),
            note: value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("dispatch did not reach a confirmed success state")
                .to_string(),
            destination: url.clone(),
            external_url: url,
            cost_snapshot: cost_gate.snapshot,
        })
    }
}

fn dispatch_message_body(args: &DispatchArgs) -> Result<String, String> {
    if let Some(message) = &args.message {
        return Ok(message.clone());
    }
    let path = args
        .message_file
        .as_ref()
        .ok_or_else(|| "approved outreach message or message file is required".to_string())?;
    std::fs::read_to_string(path).map_err(|err| format!("failed to read outreach message file: {err}"))
}

fn dispatch_idempotency_path(
    manifest: &OutreachPlatformManifest,
    args: &DispatchArgs,
) -> Result<PathBuf, String> {
    let profile = profile_dir(manifest, args.inspect.profile_root.as_deref())?;
    let root = profile.join(".openfang-outreach-dispatches");
    std::fs::create_dir_all(&root)
        .map_err(|err| format!("failed to create dispatch idempotency directory: {err}"))?;
    let mut hasher = Sha256::new();
    hasher.update(manifest.key.as_bytes());
    hasher.update(b"\n");
    hasher.update(args.inspect.source_url.as_bytes());
    hasher.update(b"\n");
    hasher.update(dispatch_message_body(args)?.trim().as_bytes());
    let key = hex::encode(hasher.finalize());
    Ok(root.join(format!("{key}.json")))
}

fn read_cached_dispatch(path: &Path) -> Result<Option<DispatchOutput>, String> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(format!(
                "failed to read dispatch idempotency metadata: {err}"
            ))
        }
    };
    if let Ok(modified) = metadata.modified() {
        if SystemTime::now()
            .duration_since(modified)
            .unwrap_or(IDEMPOTENCY_TTL + Duration::from_secs(1))
            > IDEMPOTENCY_TTL
        {
            let _ = std::fs::remove_file(path);
            return Ok(None);
        }
    }
    let raw = std::fs::read_to_string(path)
        .map_err(|err| format!("failed to read dispatch idempotency record: {err}"))?;
    let mut output: DispatchOutput = match serde_json::from_str(&raw) {
        Ok(output) => output,
        Err(_) => {
            let _ = std::fs::remove_file(path);
            return Ok(None);
        }
    };
    if output.status != "sent" {
        let _ = std::fs::remove_file(path);
        return Ok(None);
    }
    output.note = "cached dispatch result for the same source URL and message".to_string();
    Ok(Some(output))
}

fn write_cached_dispatch(path: &Path, output: &DispatchOutput) -> Result<(), String> {
    if output.status != "sent" {
        return Ok(());
    }
    let raw = serde_json::to_string_pretty(output)
        .map_err(|err| format!("failed to encode dispatch idempotency record: {err}"))?;
    std::fs::write(path, raw)
        .map_err(|err| format!("failed to write dispatch idempotency record: {err}"))
}

struct CostGate {
    snapshot: Option<Value>,
    blocked: Option<DispatchOutput>,
}

async fn verify_dispatch_cost(
    manager: &BrowserManager,
    agent_id: &str,
    manifest: &OutreachPlatformManifest,
    source_url: &str,
    expected_cost_label: Option<&str>,
) -> Result<CostGate, String> {
    let Some(expected) = expected_cost_label else {
        return Ok(CostGate {
            snapshot: None,
            blocked: None,
        });
    };
    let expression = format!(
        r#"(() => {{
const expected = {};
const normalize = (value) => String(value || '').normalize('NFKC').replace(/[\s\p{{P}}\p{{S}}]+/gu, '');
const body = document.body ? document.body.innerText : '';
const expectedNormalized = normalize(expected);
const bodyNormalized = normalize(body);
const lines = body.split(/\n+/).map((line) => line.trim()).filter(Boolean);
const matchedLines = lines
  .filter((line) => normalize(line).includes(expectedNormalized))
  .slice(0, 5);
return JSON.stringify({{
  status: expectedNormalized && bodyNormalized.includes(expectedNormalized) ? "ok" : "blocked",
  expected_cost_label: expected,
  observed_text: matchedLines.join("\n").slice(0, 1000),
  source_url: location.href,
  reason: "expected cost label was not observed on the current page before dispatch"
}});
}})()"#,
        serde_json::to_string(expected).unwrap_or_else(|_| "\"\"".to_string())
    );
    let mut value = run_js_json(manager, agent_id, &expression).await?;
    if let Value::Object(fields) = &mut value {
        fields.insert(
            "kind".to_string(),
            Value::String(manifest.cost_policy.kind.clone()),
        );
        fields.insert(
            "limit".to_string(),
            Value::String(manifest.cost_policy.limit.clone()),
        );
    }
    if value.get("status").and_then(Value::as_str) == Some("ok") {
        return Ok(CostGate {
            snapshot: Some(value),
            blocked: None,
        });
    }
    let blocked = dispatch_blocked_with_cost(
        manifest,
        value
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("expected cost label was not observed on the current page before dispatch"),
        source_url,
        Some(value.clone()),
    );
    Ok(CostGate {
        snapshot: Some(value),
        blocked: Some(blocked),
    })
}

fn expected_cost_label_for_dispatch(
    manifest: &OutreachPlatformManifest,
    args: &DispatchArgs,
) -> Result<Option<String>, &'static str> {
    if !manifest.cost_policy.required {
        return Ok(None);
    }
    let expected = args
        .expected_cost_label
        .as_deref()
        .unwrap_or_default()
        .trim();
    if expected.is_empty() {
        return Err("expected cost label is required before dispatch");
    }
    Ok(Some(expected.to_string()))
}

fn dispatch_blocked(manifest: &OutreachPlatformManifest, note: &str, url: &str) -> DispatchOutput {
    dispatch_blocked_with_cost(manifest, note, url, None)
}

fn dispatch_blocked_with_cost(
    manifest: &OutreachPlatformManifest,
    note: &str,
    url: &str,
    cost_snapshot: Option<Value>,
) -> DispatchOutput {
    DispatchOutput {
        status: "blocked".to_string(),
        adapter: format!("openfang_{}", manifest.key),
        summary: format!("{} outreach dispatch blocked.", manifest.display_name),
        note: note.to_string(),
        destination: url.to_string(),
        external_url: url.to_string(),
        cost_snapshot,
    }
}

fn resolved_selectors(
    manifest: &OutreachPlatformManifest,
) -> Result<serde_json::Map<String, Value>, String> {
    let mut selectors = serde_json::Map::new();
    for (name, env_name) in [
        ("message", &manifest.selectors.message_env),
        ("send", &manifest.selectors.send_env),
        ("success", &manifest.selectors.success_env),
    ] {
        if env_name.trim().is_empty() {
            return Err(format!("{name} selector env is missing from manifest"));
        }
        let value =
            std::env::var(env_name).map_err(|_| format!("missing selector env {env_name}"))?;
        if value.trim().is_empty() {
            return Err(format!("selector env {env_name} is empty"));
        }
        selectors.insert(name.to_string(), Value::String(value));
    }
    Ok(selectors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openfang_types::outreach::{
        BrowserProfileManifest, CostPolicy, DispatchSelectors, ReadStrategyManifest,
    };
    use std::path::PathBuf;

    fn dispatch_args(expected_cost_label: Option<&str>) -> DispatchArgs {
        DispatchArgs {
            inspect: InspectArgs {
                manifest: PathBuf::from("manifest.toml"),
                source_url: "https://www.example.com/cases/123".to_string(),
                profile_root: None,
                chromium_path: None,
                headless: true,
                json: true,
            },
            message: Some("hello".to_string()),
            message_file: None,
            expected_cost_label: expected_cost_label.map(str::to_string),
        }
    }

    fn manifest(cost_required: bool) -> OutreachPlatformManifest {
        OutreachPlatformManifest {
            key: "test".to_string(),
            display_name: "Test Platform".to_string(),
            sources: vec!["test".to_string()],
            allowed_hosts: vec!["www.example.com".to_string()],
            allowed_path_prefixes: vec!["/cases/".to_string()],
            login_url: "https://www.example.com/login".to_string(),
            auth_check_path: "/dashboard".to_string(),
            profile: BrowserProfileManifest {
                key: "test".to_string(),
                persistent: true,
            },
            read_strategy: ReadStrategyManifest::default(),
            selectors: DispatchSelectors {
                message_env: "OPENFANG_TEST_MESSAGE_SELECTOR".to_string(),
                send_env: "OPENFANG_TEST_SEND_SELECTOR".to_string(),
                success_env: "OPENFANG_TEST_SUCCESS_SELECTOR".to_string(),
            },
            cost_policy: CostPolicy {
                required: cost_required,
                kind: if cost_required { "credits" } else { "" }.to_string(),
                limit: if cost_required { "single_contact" } else { "" }.to_string(),
            },
        }
    }

    #[test]
    fn cost_required_dispatch_requires_expected_cost_label() {
        let note =
            expected_cost_label_for_dispatch(&manifest(true), &dispatch_args(None)).unwrap_err();
        assert!(note.contains("expected cost label"));
    }

    #[test]
    fn free_dispatch_does_not_require_expected_cost_label() {
        let label =
            expected_cost_label_for_dispatch(&manifest(false), &dispatch_args(None)).unwrap();
        assert!(label.is_none());
    }

    #[test]
    fn cached_sent_dispatch_is_reused_for_idempotency() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dispatch.json");
        let output = DispatchOutput {
            status: "sent".to_string(),
            adapter: "openfang_test".to_string(),
            summary: "sent".to_string(),
            note: String::new(),
            destination: "https://www.example.com/cases/123".to_string(),
            external_url: "https://www.example.com/cases/123".to_string(),
            cost_snapshot: Some(serde_json::json!({"status": "ok"})),
        };
        write_cached_dispatch(&path, &output).unwrap();

        let cached = read_cached_dispatch(&path).unwrap().unwrap();
        assert_eq!(cached.status, "sent");
        assert!(cached.note.contains("cached dispatch result"));
    }
}
