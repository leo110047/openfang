use super::runner::run_js_json;
use super::types::OutreachCapture;
use openfang_runtime::browser::BrowserManager;
use openfang_types::outreach::OutreachPlatformManifest;
use serde_json::Value;
use url::Url;

pub(super) async fn capture_current_page(
    manager: &BrowserManager,
    agent_id: &str,
    manifest: &OutreachPlatformManifest,
) -> Result<OutreachCapture, String> {
    let value = run_js_json(manager, agent_id, CAPTURE_SCRIPT).await?;
    let url = string_field(&value, "url");
    let title = string_field(&value, "title");
    let text = string_field(&value, "text");
    let login_status = login_status_from_url(&url, manifest);
    let status = classify_capture(&url, &text, manifest, &login_status);
    Ok(OutreachCapture {
        status: status.0,
        platform_key: manifest.key.clone(),
        title,
        url,
        text,
        html: string_field(&value, "html"),
        visible_actions: value
            .get("visible_actions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        important_sections: value
            .get("important_sections")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        reason: status.1,
        login_status,
    })
}

pub(super) fn classify_capture(
    url: &str,
    text: &str,
    manifest: &OutreachPlatformManifest,
    login_status: &str,
) -> (String, String) {
    if login_status == "needs_user_login" {
        return ("needs_login".to_string(), "login required".to_string());
    }
    if readable_requirement(text, manifest) {
        return ("details_ready".to_string(), String::new());
    }
    if text.trim().chars().count() < 120 {
        return (
            "blocked".to_string(),
            "page text was empty or too short".to_string(),
        );
    }
    (
        "blocked".to_string(),
        format!("no readability markers matched for allowlisted source URL {url}"),
    )
}

fn readable_requirement(text: &str, manifest: &OutreachPlatformManifest) -> bool {
    if text.trim().chars().count() < 180 {
        return false;
    }
    manifest
        .read_strategy
        .readability_markers
        .iter()
        .filter(|marker| text.contains(marker.as_str()))
        .count()
        >= 3
}

fn login_status_from_url(url: &str, manifest: &OutreachPlatformManifest) -> String {
    let parsed = match Url::parse(url) {
        Ok(parsed) => parsed,
        Err(_) => return "needs_user_login".to_string(),
    };
    let host = parsed.host_str().unwrap_or_default();
    if !manifest
        .allowed_hosts
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(host))
    {
        return "needs_user_login".to_string();
    }
    let path = parsed.path().trim_end_matches('/').to_lowercase();
    let auth_path = manifest
        .auth_check_path
        .trim_end_matches('/')
        .to_lowercase();
    if !auth_path.is_empty() && (path == auth_path || path.starts_with(&format!("{auth_path}/"))) {
        return "authenticated".to_string();
    }
    if path == "/login" || path == "/users/sign_in" {
        return "needs_user_login".to_string();
    }
    String::new()
}

fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

const CAPTURE_SCRIPT: &str = r#"(() => {
const textOf = (node) => (node && node.innerText ? node.innerText.trim() : '');
const attrSelector = (name, value) => {
  if (!value) return '';
  const escaped = String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\A ');
  return `[${name}="${escaped}"]`;
};
const selectorOf = (node) => {
  if (!node) return '';
  if (node.id) return `#${CSS.escape(node.id)}`;
  const testId = node.getAttribute && (node.getAttribute('data-testid') || node.getAttribute('data-test'));
  if (testId) return attrSelector('data-testid', testId);
  const name = node.getAttribute && node.getAttribute('name');
  if (name) return `${node.tagName.toLowerCase()}${attrSelector('name', name)}`;
  return node.tagName ? node.tagName.toLowerCase() : '';
};
const actions = Array.from(document.querySelectorAll('button, a, input[type="submit"], [role="button"]'))
  .filter((node) => {
    const rect = node.getBoundingClientRect();
    const style = window.getComputedStyle(node);
    return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
  })
  .slice(0, 80)
  .map((node) => ({
    tag: node.tagName.toLowerCase(),
    selector: selectorOf(node),
    text: textOf(node) || node.getAttribute('value') || node.getAttribute('aria-label') || '',
    href: node.href || '',
  }));
const sections = Array.from(document.querySelectorAll('main, article, section, form, [role="main"], [role="dialog"]'))
  .filter((node) => textOf(node).length >= 40)
  .slice(0, 20)
  .map((node) => ({
    tag: node.tagName.toLowerCase(),
    selector: selectorOf(node),
    text: textOf(node).slice(0, 2000),
  }));
return JSON.stringify({
  url: location.href,
  title: document.title || '',
  text: document.body ? document.body.innerText : '',
  html: document.documentElement ? document.documentElement.outerHTML.slice(0, 100000) : '',
  visible_actions: actions,
  important_sections: sections,
});
})()"#;

#[cfg(test)]
mod tests {
    use super::*;
    use openfang_types::outreach::{
        BrowserProfileManifest, CostPolicy, DispatchSelectors, OutreachPlatformManifest,
        ReadStrategyManifest,
    };

    #[test]
    fn classify_capture_requires_readable_source_text() {
        let manifest = OutreachPlatformManifest {
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
            read_strategy: ReadStrategyManifest {
                readability_markers: vec![
                    "marker-a".to_string(),
                    "marker-b".to_string(),
                    "marker-c".to_string(),
                ],
                ..ReadStrategyManifest::default()
            },
            selectors: DispatchSelectors {
                message_env: "OPENFANG_TEST_MESSAGE_SELECTOR".to_string(),
                send_env: "OPENFANG_TEST_SEND_SELECTOR".to_string(),
                success_env: "OPENFANG_TEST_SUCCESS_SELECTOR".to_string(),
            },
            cost_policy: CostPolicy {
                required: true,
                kind: "credits".to_string(),
                limit: "single_contact".to_string(),
            },
        };
        let text = "marker-a\nmarker-b\nmarker-c\n".repeat(20);
        let (status, reason) =
            classify_capture("https://www.example.com/cases/123", &text, &manifest, "");
        assert_eq!(status, "details_ready");
        assert!(reason.is_empty());
    }

    #[test]
    fn classify_capture_blocks_long_text_without_manifest_markers() {
        let manifest = OutreachPlatformManifest {
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
            read_strategy: ReadStrategyManifest {
                readability_markers: vec![
                    "marker-a".to_string(),
                    "marker-b".to_string(),
                    "marker-c".to_string(),
                ],
                ..ReadStrategyManifest::default()
            },
            selectors: DispatchSelectors {
                message_env: "OPENFANG_TEST_MESSAGE_SELECTOR".to_string(),
                send_env: "OPENFANG_TEST_SEND_SELECTOR".to_string(),
                success_env: "OPENFANG_TEST_SUCCESS_SELECTOR".to_string(),
            },
            cost_policy: CostPolicy {
                required: true,
                kind: "credits".to_string(),
                limit: "single_contact".to_string(),
            },
        };
        let text = "generic page content ".repeat(20);
        let (status, reason) =
            classify_capture("https://www.example.com/cases/123", &text, &manifest, "");
        assert_eq!(status, "blocked");
        assert!(reason.contains("no readability markers"));
    }
}
