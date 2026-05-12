# Leo Maintenance Notes

This repo is maintained as a personal fork of upstream OpenFang.

## Repository Shape

- `origin`: `https://github.com/leo110047/openfang.git`
- `upstream`: `https://github.com/RightNow-AI/openfang.git`
- `upstream` push URL is intentionally disabled.
- Personal integration branch: `leo/studio-os-runtime`
- `main` should stay aligned with `upstream/main`.

Studio OS is tracked separately:

- Local path: `/Users/leo/studio-os`
- Repo: `https://github.com/leo110047/studio-os.git`
- Runtime data, reports, logs, database files, and write token must stay out of git.

## Updating From Upstream

```bash
cd /Users/leo/openfang
git fetch upstream
git switch main
git merge --ff-only upstream/main
git switch leo/studio-os-runtime
git merge main
```

If conflicts appear, resolve them on `leo/studio-os-runtime`, then run the health stack below before pushing.

## Health Stack

Run after merges or runtime/provider/channel changes:

```bash
cargo test --workspace
cargo build --workspace --lib
cargo clippy --workspace --all-targets -- -D warnings
shellcheck scripts/install.sh
```

For Studio OS changes:

```bash
cd /Users/leo/studio-os
python3 -m unittest discover -s tests
node --check public/app.js
```

The Studio OS HTTP E2E test binds a localhost socket. If sandboxing blocks it, rerun that test outside the sandbox.

## Installing Local OpenFang Build

After a successful OpenFang build:

```bash
cd /Users/leo/openfang
cargo build -p openfang-cli
cp target/debug/openfang /Users/leo/.openfang/bin/openfang
launchctl bootout gui/$(id -u) /Users/leo/Library/LaunchAgents/ai.rightnow.openfang.plist
launchctl bootstrap gui/$(id -u) /Users/leo/Library/LaunchAgents/ai.rightnow.openfang.plist
curl -sS http://127.0.0.1:4200/api/health
```

After Studio OS changes:

```bash
launchctl bootout gui/$(id -u) /Users/leo/Library/LaunchAgents/com.leo.studio-os.plist
launchctl bootstrap gui/$(id -u) /Users/leo/Library/LaunchAgents/com.leo.studio-os.plist
curl -sS http://127.0.0.1:4310/api/health
```

## Push Workflow

OpenFang:

```bash
cd /Users/leo/openfang
git switch leo/studio-os-runtime
git status
git add <changed-files>
git commit -m "<message>"
git push
```

Studio OS:

```bash
cd /Users/leo/studio-os
git status
git add <changed-files>
git commit -m "<message>"
git push
```

