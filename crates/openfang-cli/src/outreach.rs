mod capture;
mod dispatch;
mod runner;
mod security;
mod types;

use clap::{ArgGroup, Args, Subcommand};
use openfang_types::outreach::OutreachPlatformManifest;
use serde::Serialize;
use std::path::{Path, PathBuf};

#[derive(Subcommand)]
pub enum OutreachCommands {
    /// Inspect a source URL using an outreach platform manifest.
    Inspect(InspectArgs),
    /// Open the platform login profile.
    OpenLogin(LoginArgs),
    /// Open login, wait for authentication, then inspect the source URL.
    LoginInspect(LoginInspectArgs),
    /// Verify that dispatch selectors exist on the source URL.
    VerifySelectors(InspectArgs),
    /// Dispatch an approved outreach message.
    Dispatch(DispatchArgs),
}

#[derive(Args, Clone)]
pub struct InspectArgs {
    /// Platform manifest path.
    #[arg(long)]
    manifest: PathBuf,
    /// Source URL to inspect.
    #[arg(long)]
    source_url: String,
    /// Browser profile root. Defaults to OPENFANG_OUTREACH_PROFILE_ROOT.
    #[arg(long)]
    profile_root: Option<PathBuf>,
    /// Chrome/Chromium path. Defaults to CHROME_PATH or auto-detection.
    #[arg(long)]
    chromium_path: Option<String>,
    /// Run browser headless.
    #[arg(long, default_value_t = false)]
    headless: bool,
    /// Output JSON.
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Args, Clone)]
pub struct LoginArgs {
    /// Platform manifest path.
    #[arg(long)]
    manifest: PathBuf,
    /// Browser profile root. Defaults to OPENFANG_OUTREACH_PROFILE_ROOT.
    #[arg(long)]
    profile_root: Option<PathBuf>,
    /// Chrome/Chromium path. Defaults to OPENFANG_OUTREACH_CHROME, CHROME_PATH, or auto-detection.
    #[arg(long)]
    chromium_path: Option<String>,
    /// Output JSON.
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Args, Clone)]
pub struct LoginInspectArgs {
    #[command(flatten)]
    inspect: InspectArgs,
    /// Seconds to wait for login before blocking.
    #[arg(long, default_value_t = 600)]
    wait_seconds: u64,
}

#[derive(Args, Clone)]
#[command(group(
    ArgGroup::new("message_source")
        .required(true)
        .args(["message", "message_file", "message_stdin"])
))]
pub struct DispatchArgs {
    #[command(flatten)]
    inspect: InspectArgs,
    /// Approved outreach message body.
    #[arg(long)]
    message: Option<String>,
    /// Path to a caller-owned UTF-8 temp file containing the approved message body.
    ///
    /// The caller must create this as a regular private file and remove it after dispatch.
    #[arg(long)]
    message_file: Option<PathBuf>,
    /// Read the approved outreach message body from stdin.
    #[arg(long, default_value_t = false)]
    message_stdin: bool,
    /// Cost label that must be observed on the current page before dispatch.
    #[arg(long)]
    expected_cost_label: Option<String>,
}

pub fn cmd_outreach(command: OutreachCommands) {
    let result = match command {
        OutreachCommands::Inspect(args) => run_async(async move {
            let manifest = load_manifest(&args.manifest)?;
            let capture = runner::inspect_source(&manifest, &args).await?;
            print_output(&capture, args.json)
        }),
        OutreachCommands::VerifySelectors(args) => run_async(async move {
            let manifest = load_manifest(&args.manifest)?;
            let output = dispatch::verify_selectors(&manifest, &args).await?;
            print_output(&output, args.json)
        }),
        OutreachCommands::Dispatch(args) => run_async(async move {
            let manifest = load_manifest(&args.inspect.manifest)?;
            let output = dispatch::dispatch_message(&manifest, &args).await?;
            print_output(&output, args.inspect.json)
        }),
        OutreachCommands::OpenLogin(args) => {
            let result = (|| {
                let manifest = load_manifest(&args.manifest)?;
                let profile_dir = runner::profile_dir(&manifest, args.profile_root.as_deref())?;
                let chrome = args
                    .chromium_path
                    .clone()
                    .or_else(|| std::env::var("OPENFANG_OUTREACH_CHROME").ok())
                    .or_else(|| std::env::var("CHROME_PATH").ok())
                    .ok_or_else(|| "missing chromium path for open-login".to_string())?;
                let launch = runner::launch_login_browser(&manifest, &profile_dir, &chrome)?;
                print_output(
                    &serde_json::json!({
                        "status": launch.status,
                        "platform_key": manifest.key,
                        "url": manifest.login_url,
                        "profile_dir": profile_dir,
                        "pid": launch.pid,
                    }),
                    args.json,
                )
            })();
            result
        }
        OutreachCommands::LoginInspect(args) => run_async(async move {
            let manifest = load_manifest(&args.inspect.manifest)?;
            let capture = runner::login_then_inspect(&manifest, &args).await?;
            print_output(&capture, args.inspect.json)
        }),
    };
    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run_async<F>(future: F) -> Result<(), String>
where
    F: std::future::Future<Output = Result<(), String>>,
{
    tokio::runtime::Runtime::new()
        .map_err(|err| format!("failed to start runtime: {err}"))?
        .block_on(future)
}

fn load_manifest(path: &Path) -> Result<OutreachPlatformManifest, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| format!("failed to read manifest {}: {err}", path.display()))?;
    let manifest: OutreachPlatformManifest = toml::from_str(&raw)
        .map_err(|err| format!("failed to parse manifest {}: {err}", path.display()))?;
    manifest.validate()?;
    Ok(manifest)
}

fn print_output<T: Serialize>(value: &T, json: bool) -> Result<(), String> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(value).map_err(|err| err.to_string())?
        );
    } else {
        println!(
            "{}",
            serde_json::to_string(value).map_err(|err| err.to_string())?
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct DispatchCli {
        #[command(flatten)]
        args: DispatchArgs,
    }

    fn base_args() -> Vec<&'static str> {
        vec![
            "test",
            "--manifest",
            "manifest.toml",
            "--source-url",
            "https://www.example.com/cases/123",
        ]
    }

    #[test]
    fn dispatch_message_source_group_accepts_stdin() {
        let mut args = base_args();
        args.push("--message-stdin");

        let parsed = DispatchCli::try_parse_from(args).unwrap();

        assert!(parsed.args.message_stdin);
        assert!(parsed.args.message.is_none());
        assert!(parsed.args.message_file.is_none());
    }

    #[test]
    fn dispatch_message_source_group_requires_one_source() {
        let err = match DispatchCli::try_parse_from(base_args()) {
            Ok(_) => panic!("dispatch args without a message source should fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn dispatch_message_source_group_rejects_multiple_sources() {
        let mut args = base_args();
        args.extend(["--message", "hello", "--message-file", "message.txt"]);

        let err = match DispatchCli::try_parse_from(args) {
            Ok(_) => panic!("dispatch args with multiple message sources should fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }
}
