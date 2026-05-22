mod capture;
mod dispatch;
mod runner;
mod security;
mod types;

use clap::{Args, Subcommand};
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
pub struct DispatchArgs {
    #[command(flatten)]
    inspect: InspectArgs,
    /// Approved outreach message body.
    #[arg(long)]
    message: String,
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
