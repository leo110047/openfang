//! Outreach automation contracts owned by the execution runtime.
//!
//! Studio OS owns company state and review records. These types describe the
//! OpenFang-side platform manifest that bounds browser/session/dispatch work.

use serde::{Deserialize, Serialize};
use url::Url;

const MAX_KEY_LEN: usize = 64;
const MAX_ENV_NAME_LEN: usize = 128;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct OutreachPlatformManifest {
    pub key: String,
    pub display_name: String,
    pub sources: Vec<String>,
    pub allowed_hosts: Vec<String>,
    pub allowed_path_prefixes: Vec<String>,
    pub login_url: String,
    pub auth_check_path: String,
    pub profile: BrowserProfileManifest,
    pub read_strategy: ReadStrategyManifest,
    pub selectors: DispatchSelectors,
    pub cost_policy: CostPolicy,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BrowserProfileManifest {
    pub key: String,
    pub persistent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReadStrategyManifest {
    pub wait_for_selectors: Vec<String>,
    pub important_section_selectors: Vec<String>,
    pub readability_markers: Vec<String>,
    pub max_capture_chars: usize,
}

impl Default for ReadStrategyManifest {
    fn default() -> Self {
        Self {
            wait_for_selectors: Vec::new(),
            important_section_selectors: Vec::new(),
            readability_markers: Vec::new(),
            max_capture_chars: 50_000,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DispatchSelectors {
    pub message_env: String,
    pub send_env: String,
    pub success_env: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CostPolicy {
    pub required: bool,
    pub kind: String,
    pub limit: String,
}

impl OutreachPlatformManifest {
    pub fn validate(&self) -> Result<(), String> {
        validate_key("key", &self.key)?;
        if self.display_name.trim().is_empty() {
            return Err("display_name must not be empty".to_string());
        }
        if self.sources.is_empty() {
            return Err("sources must not be empty".to_string());
        }
        for source in &self.sources {
            validate_key("source", source)?;
        }
        if self.allowed_hosts.is_empty() {
            return Err("allowed_hosts must not be empty".to_string());
        }
        for host in &self.allowed_hosts {
            validate_host(host)?;
        }
        if self.allowed_path_prefixes.is_empty() {
            return Err("allowed_path_prefixes must not be empty".to_string());
        }
        for prefix in &self.allowed_path_prefixes {
            validate_path_prefix(prefix)?;
        }
        let login_url =
            Url::parse(&self.login_url).map_err(|err| format!("login_url is invalid: {err}"))?;
        if login_url.scheme() != "https" {
            return Err("login_url must use https".to_string());
        }
        if !self.host_allowed(login_url.host_str().unwrap_or_default()) {
            return Err("login_url host must be in allowed_hosts".to_string());
        }
        if !self.auth_check_path.is_empty() {
            validate_path_prefix(&self.auth_check_path)?;
        }
        validate_key("profile.key", &self.profile.key)?;
        validate_env_name("selectors.message_env", &self.selectors.message_env)?;
        validate_env_name("selectors.send_env", &self.selectors.send_env)?;
        validate_env_name("selectors.success_env", &self.selectors.success_env)?;
        if self.cost_policy.required {
            validate_key("cost_policy.kind", &self.cost_policy.kind)?;
            validate_key("cost_policy.limit", &self.cost_policy.limit)?;
        } else if !self.cost_policy.kind.is_empty() || !self.cost_policy.limit.is_empty() {
            return Err("cost_policy kind/limit require required = true".to_string());
        }
        Ok(())
    }

    pub fn is_allowed_url(&self, url: &str) -> bool {
        let parsed = match Url::parse(url) {
            Ok(parsed) => parsed,
            Err(_) => return false,
        };
        if parsed.scheme() != "https" {
            return false;
        }
        let host = match parsed.host_str() {
            Some(host) => host,
            None => return false,
        };
        if !self.host_allowed(host) {
            return false;
        }
        if parsed.port_or_known_default() != Some(443) {
            return false;
        }
        let path = parsed.path().trim_end_matches('/');
        self.allowed_path_prefixes.iter().any(|prefix| {
            let normalized = prefix.trim_end_matches('/');
            path == normalized || path.starts_with(&format!("{normalized}/"))
        })
    }

    fn host_allowed(&self, host: &str) -> bool {
        self.allowed_hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(host))
    }
}

fn validate_key(label: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.len() > MAX_KEY_LEN {
        return Err(format!("{label} is too long"));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "{label} may only contain ASCII letters, numbers, '_' or '-'"
        ));
    }
    Ok(())
}

fn validate_host(value: &str) -> Result<(), String> {
    let trimmed = value.trim();
    if value != trimmed
        || trimmed.is_empty()
        || trimmed.len() > 253
        || value.contains('/')
        || value.contains(':')
    {
        return Err("allowed host must be a bare hostname".to_string());
    }
    for label in trimmed.split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err("allowed host must be a valid DNS hostname".to_string());
        }
        if label.starts_with('-') || label.ends_with('-') {
            return Err("allowed host labels must not start or end with '-'".to_string());
        }
        if !label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err(
                "allowed host may only contain ASCII letters, numbers, '-' and '.'".to_string(),
            );
        }
    }
    Ok(())
}

fn validate_path_prefix(value: &str) -> Result<(), String> {
    if !value.starts_with('/') || value.contains("..") {
        return Err("allowed path prefix must be absolute and must not contain '..'".to_string());
    }
    Ok(())
}

fn validate_env_name(label: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.len() > MAX_ENV_NAME_LEN {
        return Err(format!("{label} is too long"));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(format!(
            "{label} must be an uppercase environment variable name"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_manifest() -> OutreachPlatformManifest {
        toml::from_str(
            r#"
key = "pro360"
display_name = "PRO360"
sources = ["pro360", "pro360_tw"]
allowed_hosts = ["www.pro360.com.tw", "pro360.com.tw"]
allowed_path_prefixes = ["/case/", "/cases/", "/dashboard/requests/"]
login_url = "https://www.pro360.com.tw/login"
auth_check_path = "/dashboard"

[profile]
key = "pro360"
persistent = true

[read_strategy]
wait_for_selectors = ["body"]
important_section_selectors = ["main", "form"]
readability_markers = ["案件編號", "客戶預算", "您的預算"]
max_capture_chars = 50000

[selectors]
message_env = "OPENFANG_PRO360_MESSAGE_SELECTOR"
send_env = "OPENFANG_PRO360_SEND_SELECTOR"
success_env = "OPENFANG_PRO360_SUCCESS_SELECTOR"

[cost_policy]
required = true
kind = "credits"
limit = "single_contact"
"#,
        )
        .unwrap()
    }

    #[test]
    fn validates_complete_manifest() {
        let manifest = valid_manifest();
        manifest.validate().unwrap();
    }

    #[test]
    fn allows_only_manifest_urls() {
        let manifest = valid_manifest();
        assert!(manifest.is_allowed_url("https://www.pro360.com.tw/cases/445566"));
        assert!(manifest.is_allowed_url("https://www.pro360.com.tw/dashboard/requests/2523566933"));
        assert!(!manifest.is_allowed_url("http://www.pro360.com.tw/cases/445566"));
        assert!(!manifest.is_allowed_url("https://evil.example/cases/445566"));
        assert!(!manifest.is_allowed_url("https://www.pro360.com.tw:8443/cases/445566"));
        assert!(!manifest.is_allowed_url("https://www.pro360.com.tw/settings"));
    }

    #[test]
    fn rejects_missing_dispatch_selectors() {
        let mut manifest = valid_manifest();
        manifest.selectors.send_env.clear();
        let err = manifest.validate().unwrap_err();
        assert!(err.contains("selectors.send_env"));
    }

    #[test]
    fn rejects_cost_policy_without_explicit_required_flag() {
        let mut manifest = valid_manifest();
        manifest.cost_policy.required = false;
        let err = manifest.validate().unwrap_err();
        assert!(err.contains("cost_policy"));
    }

    #[test]
    fn rejects_invalid_allowed_hosts() {
        for host in [
            "www.pro360.com.tw:443",
            "www.pro360.com.tw/path",
            "bad_host.example",
            "bad host.example",
            " www.pro360.com.tw",
            "www.pro360.com.tw\n",
            "-bad.example",
            "bad-.example",
            "bad..example",
        ] {
            let mut manifest = valid_manifest();
            manifest.allowed_hosts = vec![host.to_string()];
            assert!(manifest.validate().is_err(), "{host} should be rejected");
        }
    }
}
