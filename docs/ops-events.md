# OpenFang Ops Events

OpenFang runtime errors are OpenFang infrastructure data, not Studio OS company data.

The runtime writes system errors to the local OpenFang database configured by
`[memory].sqlite_path`, or to `data_dir/openfang.db` when that override is not
set. With the default config this is:

```text
~/.openfang/data/openfang.db
└── ops_events
```

Discord is only the notification surface. When a critical/error event is
recorded, or when an actionable warning affects workflow integrity, OpenFang
also sends a Traditional Chinese alert to the configured Discord channel.
Actionable warnings currently include cron delivery failures, cron state
persistence failures, and memory embedding failures.

```text
OpenFang runtime
  ├── ops_events table: durable technical history
  └── Discord alert: immediate human notification for failures and actionable warnings

Studio OS
  └── company operating data only
```

The outbox files under `~/.openfang` are retry queues, not the long-term event
store:

```text
ops_events_outbox.jsonl
system_event_discord_alerts_outbox.jsonl
system_event_discord_alerts_state.json
```

`ops_events_outbox.jsonl` is removed after records are persisted to SQLite.
`system_event_discord_alerts_outbox.jsonl` is removed after Discord accepts the
alert. The state file prevents repeatedly notifying the same deduped error.
