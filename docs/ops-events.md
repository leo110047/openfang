# OpenFang Ops Events

OpenFang runtime errors are OpenFang infrastructure data, not Studio OS company data.

The runtime writes system errors to the local OpenFang database configured by
`[memory].sqlite_path`, or to `data_dir/openfang.db` when that override is not
set. With the default config this is:

```text
~/.openfang/data/openfang.db
└── ops_events
```

Discord is only the notification surface. When a high-severity event is recorded,
OpenFang also sends a Traditional Chinese alert to the configured Discord channel.

```text
OpenFang runtime
  ├── ops_events table: durable technical history
  └── Discord alert: immediate human notification

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
