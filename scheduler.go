package cronsched

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sun-praise/cronsched/cronexpr"
)

// HandlerFunc executes one scheduled fire. action and params come from the
// schedule definition; the returned string is stamped into the fire ledger
// as the result. A returned error is recorded as "error: <err>" — a failed
// fire never blocks the chain.
type HandlerFunc func(ctx context.Context, action, params string) (string, error)

// Scheduler drives the fire loop: query due schedules, run the handler,
// stamp results, advance chains, audit everything. The cron semantics live
// here; the Store is dumb persistence.
type Scheduler struct {
	store    Store
	handler  HandlerFunc
	now      func() time.Time
	maxSleep time.Duration
	logger   *slog.Logger
	wake     chan struct{}

	mu sync.Mutex
}

// Option configures a Scheduler.
type Option func(*Scheduler)

// WithStore sets the persistence backend. Default: MemStore.
func WithStore(s Store) Option { return func(sc *Scheduler) { sc.store = s } }

// WithHandler sets the fire executor. When nil (the default), due
// schedules are still advanced and audited — stamped "skipped: no handler
// wired" — so a due row cannot pin the loop.
func WithHandler(h HandlerFunc) Option { return func(sc *Scheduler) { sc.handler = h } }

// WithClock replaces the wall clock (for tests).
func WithClock(now func() time.Time) Option { return func(sc *Scheduler) { sc.now = now } }

// WithMaxSleep caps a single Run sleep. Default 10m. The earliest armed
// schedule shortens the sleep (with a 250ms floor); a sleep edge older
// than twice this cap is treated as stale and ignored.
func WithMaxSleep(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.maxSleep = d
		}
	}
}

// WithLogger sets the logger. Default: slog.Default().
func WithLogger(l *slog.Logger) Option { return func(sc *Scheduler) { sc.logger = l } }

// New builds a Scheduler.
func New(opts ...Option) *Scheduler {
	s := &Scheduler{
		now:      time.Now,
		maxSleep: 10 * time.Minute,
		logger:   slog.Default(),
		wake:     make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.store == nil {
		s.store = NewMemStore()
	}
	return s
}

// Add creates a schedule (upsert by name: an existing name has its cron,
// action and params replaced and is re-armed). The expression is validated
// here — a spec with no future fire time is refused at the door instead of
// becoming a permanently due row.
func (s *Scheduler) Add(ctx context.Context, name, cronSpec, action, params string) (int64, error) {
	e, err := cronexpr.Parse(cronSpec)
	if err != nil {
		return 0, fmt.Errorf("cronsched: schedule add: %w", err)
	}
	now := s.now()
	next := e.Next(now)
	if next.IsZero() {
		return 0, fmt.Errorf("cronsched: schedule add %q: cron %q never fires (no future match within the parser give-up window)",
			name, cronSpec)
	}
	return s.store.Add(ctx, Schedule{
		Name:       name,
		CronExpr:   cronSpec,
		Action:     action,
		Params:     params,
		Enabled:    true,
		NextFireAt: unixSeconds(next),
		CreatedAt:  unixSeconds(now),
	})
}

// List returns all schedules sorted by name.
func (s *Scheduler) List(ctx context.Context) ([]Schedule, error) {
	return s.store.List(ctx)
}

// Remove drops one schedule by name.
func (s *Scheduler) Remove(ctx context.Context, name string) error {
	return s.store.Remove(ctx, name)
}

// SetEnabled toggles a schedule without losing its fire chain.
func (s *Scheduler) SetEnabled(ctx context.Context, name string, enabled bool) error {
	return s.store.SetEnabled(ctx, name, enabled)
}

// TriggerNow force-arms a schedule: it fires on the next Tick.
func (s *Scheduler) TriggerNow(ctx context.Context, name string) error {
	return s.store.TriggerNow(ctx, name)
}

// RecentEvents returns the audit tail, newest first.
func (s *Scheduler) RecentEvents(ctx context.Context, limit int) ([]Event, error) {
	return s.store.RecentEvents(ctx, limit)
}

// Tick executes every due schedule once: run handler, stamp result, advance
// (or disable) the chain, write the audit row. Missed slots do not catch
// up — the next future slot wins, so downtime never produces a burst.
// Returns the number of due entries processed.
func (s *Scheduler) Tick(ctx context.Context) int {
	now := s.now()
	due, err := s.store.Due(ctx, unixSeconds(now))
	if err != nil {
		s.logger.Error("cronsched: due schedules", "err", err)
		return 0
	}
	for _, entry := range due {
		s.fireOne(ctx, entry, now)
	}
	return len(due)
}

func (s *Scheduler) fireOne(ctx context.Context, entry Schedule, now time.Time) {
	var (
		result string
		took   float64
	)

	// The chain advances even when the spec has rotted in the store: a
	// due row left un-stamped stays due, and the wake loop's 250ms floor
	// would hammer ticks forever. An unparsable spec is fail-closed —
	// disable instead of guessing a cadence.
	expr, parseErr := cronexpr.Parse(entry.CronExpr)
	var next time.Time
	if parseErr != nil {
		result = "error: unparsable cron: " + parseErr.Error()
	} else {
		next = expr.Next(now)
	}

	switch {
	case parseErr != nil:
		// result already set above.
	case s.handler == nil:
		result = "skipped: no handler wired"
		s.logger.Warn("cronsched: due schedule skipped (no handler wired)",
			"name", entry.Name, "action", entry.Action)
	default:
		start := s.now()
		res, err := s.handler(ctx, entry.Action, entry.Params)
		took = s.now().Sub(start).Seconds()
		if err != nil {
			result = "error: " + err.Error()
			s.logger.Error("cronsched: schedule fired with error",
				"name", entry.Name, "action", entry.Action, "err", err)
		} else {
			result = res
			s.logger.Info("cronsched: schedule fired",
				"name", entry.Name, "action", entry.Action, "result", result)
		}
	}

	disabledNote := ""
	if next.IsZero() && parseErr == nil {
		disabledNote = "; disabled: cron has no future fire time"
	}
	nextAt := unixSeconds(next)
	if err := s.store.RecordFire(ctx, entry.ID, result+disabledNote, took, nextAt); err != nil {
		s.logger.Error("cronsched: record schedule fire", "name", entry.Name, "err", err)
	}
}

// Run loops Tick + wake-edge sleep until ctx is cancelled (returning
// ctx's error). The sleep is min(maxSleep, earliest armed schedule), with
// a 250ms floor so a due-now edge wakes near-immediately without a hot
// spin; Wake interrupts the sleep early.
func (s *Scheduler) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.Tick(ctx)
		sleep := s.nextSleep(ctx, s.now())
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-s.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Wake interrupts the current sleep so the next Tick starts immediately.
func (s *Scheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// nextSleep computes the wait until the next Tick: min(maxSleep, earliest
// armed schedule). A due-now edge gets a 250ms floor (near-immediate, no
// hot spin); an edge older than the staleness bound (2× maxSleep, at least
// 10m) has missed its point and falls back to maxSleep.
func (s *Scheduler) nextSleep(ctx context.Context, now time.Time) time.Duration {
	sleep := s.maxSleep
	stale := 2 * s.maxSleep
	if stale < 10*time.Minute {
		stale = 10 * time.Minute
	}
	at, ok := s.store.Earliest(ctx)
	if !ok {
		return sleep
	}
	wait := time.UnixMilli(int64(at * 1000)).Sub(now)
	if wait < -stale {
		return sleep
	}
	if wait < 250*time.Millisecond {
		wait = 250 * time.Millisecond
	}
	if wait < sleep {
		sleep = wait
	}
	return sleep
}
