// Package cronsched is an embedded, persistent cron scheduler for Go
// single-binary daemons.
//
// Schedules live in a Store instead of in process memory: they survive
// restarts, every fire lands in an audit ledger, and Run sleeps until the
// earliest wake edge instead of polling. The default SQLite backend is
// pure Go (no CGO) and keeps the whole state in one file.
//
// The layering is strict: the Scheduler owns cron semantics (parsing,
// validation, chain advancement, disable-on-exhaustion); a Store is dumb
// persistence that can be swapped for any backend.
package cronsched

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by the name-addressed operations when no
// schedule with that name exists.
var ErrNotFound = errors.New("cronsched: schedule not found")

// Schedule is one recurring cron action.
type Schedule struct {
	ID          int64
	Name        string
	CronExpr    string
	Action      string
	Params      string
	Enabled     bool
	NextFireAt  float64 // unix seconds; 0 = unset
	LastFiredAt float64 // unix seconds; 0 = never fired
	LastResult  string
	CreatedAt   float64
}

// Event is one audit row per fire.
type Event struct {
	ID              int64
	ScheduleID      int64
	Name            string
	FiredAt         float64
	Action          string
	Result          string
	DurationSeconds float64
}

// Store is the persistence seam behind a Scheduler. Implementations must
// be safe for concurrent use.
//
// Semantics (shared by the provided implementations; a custom Store should
// match them):
//
//   - Add upserts by Name: an existing row has cron/action/params replaced
//     and is re-enabled and re-armed with the given NextFireAt; the row's
//     ID is unchanged.
//   - Due returns enabled schedules with NextFireAt > 0 and <= now,
//     oldest due first.
//   - RecordFire stamps the fire outcome and writes an audit Event. A
//     nextFireAt > 0 advances the chain; <= 0 means the chain is exhausted
//     and the schedule must be disabled (a disabled row is never Due).
//     The audit row must land even when ctx is cancelled — durability
//     outranks the caller's deadline here.
//   - TriggerNow arms a schedule to fire on the next Due query; only
//     enabled schedules can be armed.
//   - Earliest returns the soonest armed NextFireAt (the run loop sleeps
//     no longer than this), or false when nothing is armed.
type Store interface {
	Add(ctx context.Context, s Schedule) (int64, error)
	List(ctx context.Context) ([]Schedule, error)
	Remove(ctx context.Context, name string) error
	SetEnabled(ctx context.Context, name string, enabled bool) error
	Due(ctx context.Context, now float64) ([]Schedule, error)
	RecordFire(ctx context.Context, id int64, result string, tookSeconds float64, nextFireAt float64) error
	TriggerNow(ctx context.Context, name string) error
	RecentEvents(ctx context.Context, limit int) ([]Event, error)
	Earliest(ctx context.Context) (float64, bool)
}

// unixSeconds converts a time to unix seconds (fractional). The zero time
// maps to 0.
func unixSeconds(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.UnixMilli()) / 1000.0
}
