package cronsched

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fixedClock returns a clock function around a mutable time.
func fixedClock(start time.Time) (func() time.Time, func(time.Time), func(time.Duration)) {
	var mu atomic.Value
	mu.Store(start)
	get := func() time.Time { return mu.Load().(time.Time) }
	set := func(t time.Time) { mu.Store(t) }
	advance := func(d time.Duration) {
		cur := mu.Load().(time.Time)
		mu.Store(cur.Add(d))
	}
	return get, set, advance
}

func TestSchedulerAddValidates(t *testing.T) {
	clock, _, _ := fixedClock(time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC))
	s := New(WithClock(clock))

	if _, err := s.Add(context.Background(), "impossible", "0 0 30 2 *", "x", ""); err == nil {
		t.Fatal("never-firing cron accepted at Add")
	}
	if _, err := s.Add(context.Background(), "fast", "@every 30s", "x", ""); err == nil {
		t.Fatal("sub-minute @every accepted at Add")
	}
	if _, err := s.Add(context.Background(), "ok", "0 2 * * *", "backup", "{}"); err != nil {
		t.Fatal(err)
	}
	all, err := s.List(context.Background())
	if err != nil || len(all) != 1 {
		t.Fatalf("List = %+v, err %v", all, err)
	}
	// Next slot strictly after 2026-10-18 12:00 UTC is 2026-10-19 02:00.
	if want := unixSeconds(time.Date(2026, 10, 19, 2, 0, 0, 0, time.UTC)); all[0].NextFireAt != want {
		t.Fatalf("NextFireAt = %v, want %v", all[0].NextFireAt, want)
	}
}

func TestSchedulerTickFiresDueAndAdvances(t *testing.T) {
	clock, _, advance := fixedClock(time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC))
	var gotAction, gotParams atomic.Value
	s := New(
		WithClock(func() time.Time { return clock() }),
		WithHandler(func(ctx context.Context, action, params string) (string, error) {
			gotAction.Store(action)
			gotParams.Store(params)
			return "ok", nil
		}),
	)
	if _, err := s.Add(context.Background(), "nightly", "0 2 * * *", "backup", `{"dst":"/tmp"}`); err != nil {
		t.Fatal(err)
	}
	// Before the fire time: nothing due.
	if n := s.Tick(context.Background()); n != 0 {
		t.Fatalf("Tick before fire time processed %d entries", n)
	}
	// Cross the fire time.
	advance(2 * time.Hour)
	if n := s.Tick(context.Background()); n != 1 {
		t.Fatalf("Tick processed %d entries, want 1", n)
	}
	if gotAction.Load() != "backup" || gotParams.Load() != `{"dst":"/tmp"}` {
		t.Fatalf("handler args = %v %v", gotAction.Load(), gotParams.Load())
	}
	all, _ := s.List(context.Background())
	e := all[0]
	if e.NextFireAt <= unixSeconds(clock()) {
		t.Fatalf("chain not advanced past now: %v", e.NextFireAt)
	}
	if e.LastResult != "ok" || e.LastFiredAt <= 0 {
		t.Fatalf("fire not stamped: %+v", e)
	}
	// Idempotent within the same instant: no double fire.
	if n := s.Tick(context.Background()); n != 0 {
		t.Fatalf("re-Tick at the same instant fired again (%d)", n)
	}
}

func TestSchedulerTickSkipsMissedSlots(t *testing.T) {
	clock, _, advance := fixedClock(time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC))
	var fires atomic.Int32
	s := New(
		WithClock(func() time.Time { return clock() }),
		WithHandler(func(ctx context.Context, action, params string) (string, error) {
			fires.Add(1)
			return "ok", nil
		}),
	)
	// Hourly cron armed at 00:00 → next slot 01:00.
	if _, err := s.Add(context.Background(), "hourly", "0 * * * *", "x", ""); err != nil {
		t.Fatal(err)
	}
	// Four days of downtime: slots at 01:00..96:00 are all missed.
	advance(96 * time.Hour)
	if n := s.Tick(context.Background()); n != 1 {
		t.Fatalf("Tick processed %d entries, want 1", n)
	}
	if got := fires.Load(); got != 1 {
		t.Fatalf("catch-up burst: handler ran %d times, want exactly 1", got)
	}
	all, _ := s.List(context.Background())
	// Next slot is strictly in the future (04:00 on day 5), not a replay.
	if all[0].NextFireAt <= unixSeconds(clock()) {
		t.Fatalf("next fire not in the future: %v vs %v", all[0].NextFireAt, unixSeconds(clock()))
	}
}

func TestSchedulerHandlerErrorStillAdvances(t *testing.T) {
	clock, _, advance := fixedClock(time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC))
	s := New(
		WithClock(func() time.Time { return clock() }),
		WithHandler(func(ctx context.Context, action, params string) (string, error) {
			return "", errors.New("boom")
		}),
	)
	if _, err := s.Add(context.Background(), "j", "0 2 * * *", "x", ""); err != nil {
		t.Fatal(err)
	}
	advance(2 * time.Hour)
	s.Tick(context.Background())
	all, _ := s.List(context.Background())
	if !strings.HasPrefix(all[0].LastResult, "error: ") {
		t.Fatalf("LastResult = %q, want error prefix", all[0].LastResult)
	}
	if !all[0].Enabled || all[0].NextFireAt <= unixSeconds(clock()) {
		t.Fatalf("failed fire must not block the chain: %+v", all[0])
	}
}

func TestSchedulerNilHandlerSkipsButAdvances(t *testing.T) {
	clock, _, advance := fixedClock(time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC))
	s := New(WithClock(func() time.Time { return clock() }))
	if _, err := s.Add(context.Background(), "j", "0 2 * * *", "x", ""); err != nil {
		t.Fatal(err)
	}
	advance(2 * time.Hour)
	s.Tick(context.Background())
	all, _ := s.List(context.Background())
	if all[0].LastResult != "skipped: no handler wired" {
		t.Fatalf("LastResult = %q", all[0].LastResult)
	}
	if !all[0].Enabled || all[0].NextFireAt <= unixSeconds(clock()) {
		t.Fatalf("skip must still advance the chain: %+v", all[0])
	}
}

// TestSchedulerRottenSpecFailsClosed seeds a store row whose cron parses
// but never fires again, and one that does not parse at all — both must be
// stamped and disabled, never left permanently due.
func TestSchedulerRottenSpecFailsClosed(t *testing.T) {
	clock, _, _ := fixedClock(time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC))
	mem := NewMemStore()
	s := New(WithClock(func() time.Time { return clock() }), WithStore(mem))
	past := float64(clock().Add(-time.Hour).Unix())
	if _, err := mem.Add(context.Background(), Schedule{Name: "impossible",
		CronExpr: "0 0 30 2 *", Action: "x", Enabled: true, NextFireAt: past, CreatedAt: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Add(context.Background(), Schedule{Name: "garbage",
		CronExpr: "not a spec", Action: "x", Enabled: true, NextFireAt: past, CreatedAt: 1}); err != nil {
		t.Fatal(err)
	}
	if n := s.Tick(context.Background()); n != 2 {
		t.Fatalf("Tick processed %d, want 2", n)
	}
	all, _ := s.List(context.Background())
	for _, e := range all {
		if e.Enabled {
			t.Fatalf("%s: rotten spec left enabled (%q)", e.Name, e.LastResult)
		}
		if e.Name == "garbage" && !strings.HasPrefix(e.LastResult, "error: unparsable cron") {
			t.Fatalf("garbage result = %q", e.LastResult)
		}
		if e.Name == "impossible" && !strings.Contains(e.LastResult, "disabled: cron has no future fire time") {
			t.Fatalf("impossible result = %q", e.LastResult)
		}
	}
}

func TestSchedulerRunFiresAndStops(t *testing.T) {
	clock, _, advance := fixedClock(time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC))
	fired := make(chan struct{}, 1)
	s := New(
		WithClock(func() time.Time { return clock() }),
		WithMaxSleep(50*time.Millisecond),
		WithHandler(func(ctx context.Context, action, params string) (string, error) {
			select {
			case fired <- struct{}{}:
			default:
			}
			return "ok", nil
		}),
	)
	if _, err := s.Add(context.Background(), "j", "0 2 * * *", "x", ""); err != nil {
		t.Fatal(err)
	}
	advance(2 * time.Hour) // due immediately

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("Run never fired the due schedule")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

// TestWakeInterruptsSleep pins the wake-edge property: Wake() shortens the
// current sleep, observable as an immediate second pass over the store.
type countingStore struct {
	Store
	dueCalls atomic.Int32
}

func (c *countingStore) Due(ctx context.Context, now float64) ([]Schedule, error) {
	c.dueCalls.Add(1)
	return c.Store.Due(ctx, now)
}

func TestWakeInterruptsSleep(t *testing.T) {
	clock, _, _ := fixedClock(time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC))
	cs := &countingStore{Store: NewMemStore()}
	s := New(
		WithClock(func() time.Time { return clock() }),
		WithStore(cs),
		WithMaxSleep(10*time.Second),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// First Tick happens at once; the loop then sleeps up to 10s.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cs.dueCalls.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if cs.dueCalls.Load() < 1 {
		t.Fatal("first Tick never ran")
	}
	s.Wake()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cs.dueCalls.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if cs.dueCalls.Load() < 2 {
		t.Fatal("Wake did not shorten the sleep")
	}
	cancel()
	<-done
}
