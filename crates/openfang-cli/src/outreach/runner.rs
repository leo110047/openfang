use super::capture::capture_current_page;
use super::security::validate_platform_url;
use super::types::{OutreachCapture, ProfileLock};
use super::{InspectArgs, LoginInspectArgs};
use openfang_runtime::browser::{BrowserCommand, BrowserManager};
use openfang_types::config::BrowserConfig;
use openfang_types::outreach::OutreachPlatformManifest;
use serde_json::{json, Value};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LOGIN_BROWSER_STALE_SECONDS: u64 = 4 * 60 * 60;

pub(super) struct LoginBrowserLaunch {
    pub status: String,
    pub pid: u32,
}

pub(super) async fn inspect_source(
    manifest: &OutreachPlatformManifest,
    args: &InspectArgs,
) -> Result<OutreachCapture, String> {
    validate_platform_url(&args.source_url, manifest)?;
    let _lock = lock_profile(manifest, args.profile_root.as_deref())?;
    let manager = BrowserManager::new(browser_config(manifest, args)?);
    let agent_id = format!("outreach-{}", manifest.key);
    navigate(&manager, &agent_id, &args.source_url).await?;
    let capture = capture_current_page(&manager, &agent_id, manifest).await;
    manager.close_session(&agent_id).await;
    capture
}

pub(super) async fn login_then_inspect(
    manifest: &OutreachPlatformManifest,
    args: &LoginInspectArgs,
) -> Result<OutreachCapture, String> {
    validate_platform_url(&args.inspect.source_url, manifest)?;
    let _lock = lock_profile(manifest, args.inspect.profile_root.as_deref())?;
    let manager = BrowserManager::new(browser_config(manifest, &args.inspect)?);
    let agent_id = format!("outreach-{}", manifest.key);
    navigate(&manager, &agent_id, &manifest.login_url).await?;
    let deadline = Instant::now() + Duration::from_secs(args.wait_seconds.max(1));
    loop {
        let capture = capture_current_page(&manager, &agent_id, manifest).await?;
        if capture.login_status == "authenticated" {
            break;
        }
        if Instant::now() >= deadline {
            manager.close_session(&agent_id).await;
            return Ok(OutreachCapture {
                status: "needs_login".to_string(),
                platform_key: manifest.key.clone(),
                title: capture.title,
                url: capture.url,
                text: capture.text,
                html: capture.html,
                visible_actions: capture.visible_actions,
                important_sections: capture.important_sections,
                reason: format!(
                    "{} login was not completed before timeout",
                    manifest.display_name
                ),
                login_status: "needs_user_login".to_string(),
            });
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    navigate(&manager, &agent_id, &args.inspect.source_url).await?;
    let capture = capture_current_page(&manager, &agent_id, manifest).await;
    manager.close_session(&agent_id).await;
    capture
}

pub(super) fn profile_dir(
    manifest: &OutreachPlatformManifest,
    override_root: Option<&Path>,
) -> Result<PathBuf, String> {
    let root = override_root
        .map(Path::to_path_buf)
        .or_else(|| {
            std::env::var("OPENFANG_OUTREACH_PROFILE_ROOT")
                .ok()
                .map(PathBuf::from)
        })
        .or_else(|| dirs::home_dir().map(|home| home.join(".openfang").join("outreach-profiles")))
        .ok_or_else(|| "could not resolve outreach profile root".to_string())?;
    Ok(root.join(&manifest.profile.key))
}

pub(super) fn launch_login_browser(
    manifest: &OutreachPlatformManifest,
    profile_dir: &Path,
    chrome_path: &str,
) -> Result<LoginBrowserLaunch, String> {
    std::fs::create_dir_all(profile_dir)
        .map_err(|err| format!("failed to create profile dir: {err}"))?;
    let pid_path = login_browser_pid_path(profile_dir);
    if let Some(pid) = live_login_browser_pid(&pid_path)? {
        return Ok(LoginBrowserLaunch {
            status: "already_open".to_string(),
            pid,
        });
    }
    let child = Command::new(chrome_path)
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg(&manifest.login_url)
        .spawn()
        .map_err(|err| format!("failed to launch login browser: {err}"))?;
    let pid = child.id();
    write_login_browser_pid(&pid_path, pid, chrome_path, &manifest.login_url)?;
    Ok(LoginBrowserLaunch {
        status: "opening".to_string(),
        pid,
    })
}

pub(super) async fn run_js_json(
    manager: &BrowserManager,
    agent_id: &str,
    expression: &str,
) -> Result<Value, String> {
    let response = manager
        .send_command(
            agent_id,
            BrowserCommand::RunJs {
                expression: expression.to_string(),
            },
        )
        .await?;
    if !response.success {
        return Err(response
            .error
            .unwrap_or_else(|| "javascript execution failed".to_string()));
    }
    let data = response.data.unwrap_or_default();
    let result = data.get("result").cloned().unwrap_or(Value::Null);
    if let Some(raw) = result.as_str() {
        serde_json::from_str(raw).map_err(|err| format!("javascript returned invalid JSON: {err}"))
    } else {
        Ok(result)
    }
}

pub(super) async fn navigate(
    manager: &BrowserManager,
    agent_id: &str,
    url: &str,
) -> Result<(), String> {
    let response = manager
        .send_command(
            agent_id,
            BrowserCommand::Navigate {
                url: url.to_string(),
            },
        )
        .await?;
    if response.success {
        Ok(())
    } else {
        Err(response
            .error
            .unwrap_or_else(|| "navigation failed".to_string()))
    }
}

pub(super) fn browser_config(
    manifest: &OutreachPlatformManifest,
    args: &InspectArgs,
) -> Result<BrowserConfig, String> {
    let profile = profile_dir(manifest, args.profile_root.as_deref())?;
    Ok(BrowserConfig {
        headless: args.headless,
        chromium_path: args.chromium_path.clone(),
        user_data_dir: Some(profile.to_string_lossy().to_string()),
        ..BrowserConfig::default()
    })
}

pub(super) fn lock_profile(
    manifest: &OutreachPlatformManifest,
    override_root: Option<&Path>,
) -> Result<ProfileLock, String> {
    let dir = profile_dir(manifest, override_root)?;
    std::fs::create_dir_all(&dir).map_err(|err| format!("failed to create profile dir: {err}"))?;
    let path = dir.join(".openfang-outreach.lock");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|err| format!("failed to open profile lock: {err}"))?;
    acquire_profile_file_lock(&file, &manifest.profile.key)?;
    write_profile_lock_metadata(&mut file)?;
    Ok(ProfileLock {
        path,
        #[cfg(unix)]
        file,
    })
}

#[cfg(unix)]
fn acquire_profile_file_lock(file: &File, profile_key: &str) -> Result<(), String> {
    use std::os::fd::AsRawFd;
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    let raw_os_error = err.raw_os_error();
    if raw_os_error == Some(libc::EWOULDBLOCK) || raw_os_error == Some(libc::EAGAIN) {
        return Err(format!("profile {profile_key} is already in use"));
    }
    Err(format!("failed to lock profile {profile_key}: {err}"))
}

#[cfg(not(unix))]
fn acquire_profile_file_lock(_file: &File, _profile_key: &str) -> Result<(), String> {
    Ok(())
}

fn write_profile_lock_metadata(file: &mut File) -> Result<(), String> {
    file.set_len(0)
        .map_err(|err| format!("failed to reset profile lock metadata: {err}"))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|err| format!("failed to seek profile lock metadata: {err}"))?;
    let metadata = json!({
        "pid": std::process::id(),
        "locked_at_unix": unix_now(),
    });
    file.write_all(metadata.to_string().as_bytes())
        .map_err(|err| format!("failed to write profile lock metadata: {err}"))?;
    file.sync_data()
        .map_err(|err| format!("failed to sync profile lock metadata: {err}"))?;
    Ok(())
}

fn login_browser_pid_path(profile_dir: &Path) -> PathBuf {
    profile_dir.join(".openfang-outreach-login.json")
}

fn live_login_browser_pid(pid_path: &Path) -> Result<Option<u32>, String> {
    let Some(record) = read_login_browser_pid(pid_path)? else {
        return Ok(None);
    };
    if process_is_alive(record.pid) && !login_browser_record_expired(record.launched_at_unix) {
        return Ok(Some(record.pid));
    }
    let _ = std::fs::remove_file(pid_path);
    Ok(None)
}

struct LoginBrowserRecord {
    pid: u32,
    launched_at_unix: u64,
}

fn read_login_browser_pid(pid_path: &Path) -> Result<Option<LoginBrowserRecord>, String> {
    let mut file = match File::open(pid_path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("failed to read login browser pid file: {err}")),
    };
    let mut raw = String::new();
    file.read_to_string(&mut raw)
        .map_err(|err| format!("failed to read login browser pid file: {err}"))?;
    let value: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => {
            let _ = std::fs::remove_file(pid_path);
            return Ok(None);
        }
    };
    let Some(pid) = value.get("pid").and_then(Value::as_u64) else {
        let _ = std::fs::remove_file(pid_path);
        return Ok(None);
    };
    Ok(Some(LoginBrowserRecord {
        pid: pid as u32,
        launched_at_unix: value
            .get("launched_at_unix")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }))
}

fn write_login_browser_pid(
    pid_path: &Path,
    pid: u32,
    chrome_path: &str,
    url: &str,
) -> Result<(), String> {
    let metadata = json!({
        "pid": pid,
        "launched_at_unix": unix_now(),
        "chrome_path": chrome_path,
        "url": url,
    });
    let mut raw = metadata.to_string();
    raw.push('\n');
    std::fs::write(pid_path, raw)
        .map_err(|err| format!("failed to write login browser pid file: {err}"))
}

fn login_browser_record_expired(launched_at_unix: u64) -> bool {
    let now = unix_now();
    launched_at_unix == 0 || now.saturating_sub(launched_at_unix) > LOGIN_BROWSER_STALE_SECONDS
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    rc == 0
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use openfang_types::outreach::{
        BrowserProfileManifest, CostPolicy, DispatchSelectors, OutreachPlatformManifest,
        ReadStrategyManifest,
    };

    fn manifest() -> OutreachPlatformManifest {
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
                required: true,
                kind: "credits".to_string(),
                limit: "single_contact".to_string(),
            },
        }
    }

    #[test]
    fn profile_lock_recovers_from_stale_lock_file() {
        let root = tempfile::tempdir().unwrap();
        let profile = profile_dir(&manifest(), Some(root.path())).unwrap();
        std::fs::create_dir_all(&profile).unwrap();
        std::fs::write(profile.join(".openfang-outreach.lock"), "stale").unwrap();

        let lock = lock_profile(&manifest(), Some(root.path())).unwrap();
        drop(lock);
    }

    #[test]
    fn login_browser_pid_record_expires_stale_processes() {
        assert!(login_browser_record_expired(0));
        assert!(!login_browser_record_expired(unix_now()));
    }
}
