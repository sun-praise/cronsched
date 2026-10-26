# cronsched

[![CI](https://github.com/sun-praise/cronsched/actions/workflows/ci.yml/badge.svg)](https://github.com/sun-praise/cronsched/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/sun-praise/cronsched/graph/badge.svg)](https://codecov.io/gh/sun-praise/cronsched)
[![Go Reference](https://pkg.go.dev/badge/github.com/sun-praise/cronsched.svg)](https://pkg.go.dev/github.com/sun-praise/cronsched)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

An embedded, persistent cron scheduler for Go single-binary daemons.

`cronsched` is a cron scheduler whose state lives in a store instead of in
process memory: schedules survive restarts, every fire lands in an audit
ledger, and the run loop sleeps until the earliest wake edge instead of
polling. The default storage backend is a single SQLite file (pure Go, no
CGO), which makes the whole thing a drop-in for self-hosted tools, agent
runners, and ops utilities that don't want to require Redis or Postgres.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/sun-praise/cronsched"
)

func main() {
	s := cronsched.New(
		cronsched.WithStore(cronsched.NewSQLiteStore("sched.db")),
		cronsched.WithHandler(func(ctx context.Context, action, params string) (string, error) {
			fmt.Println("fired:", action, params)
			return "ok", nil
		}),
	)
	if _, err := s.Add(context.Background(), "nightly", "0 2 * * *", "backup", `{"dst":"/tmp"}`); err != nil {
		log.Fatal(err)
	}
	s.Run(context.Background())
}
```

## Features

- **Persistent and recoverable** — schedule definitions and the fire chain
  live in a `Store`; after a restart, due-but-unfired schedules are visible
  and the chain advances without catch-up bursts (missed slots are skipped,
  the next future slot wins).
- **Fire ledger** — every fire (success, error, skip, manual trigger) writes
  an audit row: when, what, result, duration. A scheduler that also remembers
  what it did.
- **Wake-edge sleeping** — `Run` sleeps until `min(max sleep, earliest due
  schedule)`, with a 250 ms floor and a staleness ceiling, so a due schedule
  wakes the loop near-immediately and there is no hot spin.
- **Pure Go, zero deployment** — the SQLite backend uses `modernc.org/sqlite`
  (no CGO). Full dependency tree: `robfig/cron` (MIT) + `modernc.org/sqlite`
  (BSD-3). Nothing else.
- **Deterministic tests** — inject a clock (`WithClock`), use the in-memory
  store, drive `Tick` by hand.

## Usage

### Use the database your program already has

One option over your existing `*sql.DB` — SQLite, MySQL or Postgres. The
dialect is detected from the driver name (overridable with `WithDialect`),
and the tables are created on first use:

```go
db := myApp.OpenDB() // your existing handle; any database/sql driver
s := cronsched.New(cronsched.WithSQLStore(db))
```

### Zero-config single file

```go
s := cronsched.New(cronsched.WithStore(cronsched.NewSQLiteStore("sched.db")))
```

### Add / inspect / control

```go
id, err := s.Add(ctx, "nightly", "0 2 * * *", "backup", `{}`) // upsert by name, (re)arms
schedules, err := s.List(ctx)
err = s.SetEnabled(ctx, "nightly", false)  // keeps the fire chain
err = s.Remove(ctx, "nightly")
err = s.TriggerNow(ctx, "nightly")         // force-arm: fires on the next Tick
events, err := s.RecentEvents(ctx, 20)     // audit tail, newest first
```

### Drive it yourself

`Tick` is the whole scheduling step — query due schedules, run the handler
once per entry, stamp results, advance chains. Embed it in your own loop if
`Run`'s sleep policy doesn't fit:

```go
for {
	s.Tick(ctx)
	waitForOtherWakeSources(ctx)
}
```

### Bring your own store

`Store` is a small interface (9 methods). `MemStore`, `SQLiteStore` and the
generic `SQLStore` are provided; anything else — NoSQL, in-memory variants,
your company's secret database — implements the interface, and the
[storetest](https://pkg.go.dev/github.com/sun-praise/cronsched/storetest)
subpackage gives you the exact contract suite CI runs against the built-ins.

## Expressions

The `cronexpr` subpackage wraps `robfig/cron/v3`'s standard parser:
five fields (minute hour dom mon dow), lists/ranges/steps, named months and
weekdays (`MON-FRI`), `@hourly`/`@daily`/`@every 2h` descriptors, and
`CRON_TZ=` prefixes. Vixie's day-of-week alias `7` (Sunday) is folded
correctly, including stepped ranges (`1-7/2` = Mon,Wed,Fri,Sun).

One deliberate restriction: `@every` below one minute is rejected at parse
time. Sub-minute `@every` combined with a fast-wake loop degrades into a
per-cycle batch; if you need sub-minute cadence, use a ticker instead.

## What this is not

- **Not a distributed scheduler** — no multi-node preemption, leader
  election, or shard assignment. One process owns one store.
- **Not a task queue** — no message delivery, no worker pools. Handlers run
  inline on the scheduler goroutine; long or parallel work is the handler's
  job to dispatch.
- **Not an in-memory cron replacement** — if you don't need persistence or
  an audit trail, `robfig/cron` or `go-co-op/gocron` are simpler.

## Related projects

The scheduling landscape cronsched grew out of — each is worth a look, and
several are the better tool for their niche:

- [robfig/cron](https://github.com/robfig/cron) — the classic in-memory
  cron; its standard parser powers the `cronexpr` subpackage. In-memory
  only: state is lost on restart.
- [go-co-op/gocron](https://github.com/go-co-op/gocron) — fluent in-memory
  job scheduling with a friendly API.
- [reugn/go-quartz](https://github.com/reugn/go-quartz) — Quartz-inspired
  trigger abstraction, zero dependencies.
- [riverqueue/river](https://github.com/riverqueue/river) — Postgres/SQLite
  job queue with periodic jobs (the periodic schedule itself is held in
  memory). The stronger tool when you need a full worker-pool queue.
- [hibiken/asynq](https://github.com/hibiken/asynq) — Redis-backed
  distributed task queue with periodic tasks; requires Redis.
- [AGScheduler](https://agscheduler.github.io/agscheduler/) — scheduling
  framework with persistence, clustering and remote calls.

cronsched occupies the gap between them: an embedded cron library whose
schedules and fire history persist without asking you to run anything.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
