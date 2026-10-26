// Package storetest exposes the cronsched Store contract suite so any
// backend — in-tree or out-of-tree — can prove it honours the semantics
// the Scheduler relies on.
//
// Usage:
//
//	func TestMyStoreContract(t *testing.T) {
//	    storetest.Contract(t, func(t *testing.T) cronsched.Store {
//	        return NewMyStore(t) // provision isolated state per subtest
//	    })
//	}
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sun-praise/cronsched"
)

// Contract runs the full Store semantics suite. newStore is called once
// per subtest and must return a store with isolated state (a fresh file,
// a truncated table, ...); it receives the subtest's t for provisioning
// and cleanup.
func Contract(t *testing.T, newStore func(t *testing.T) cronsched.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("add upserts by name", func(t *testing.T) {
		st := newStore(t)
		id1, err := st.Add(ctx, cronsched.Schedule{Name: "a", CronExpr: "0 2 * * *",
			Action: "x", Params: "1", Enabled: true, NextFireAt: 100, CreatedAt: 1})
		if err != nil {
			t.Fatal(err)
		}
		id2, err := st.Add(ctx, cronsched.Schedule{Name: "b", CronExpr: "0 3 * * *",
			Action: "y", Params: "", Enabled: true, NextFireAt: 200, CreatedAt: 1})
		if err != nil {
			t.Fatal(err)
		}
		if id1 == id2 {
			t.Fatalf("distinct schedules share id %d", id1)
		}
		// Re-add the same name: same row, new fields, re-armed.
		id3, err := st.Add(ctx, cronsched.Schedule{Name: "a", CronExpr: "30 4 * * *",
			Action: "x2", Params: "2", Enabled: true, NextFireAt: 300, CreatedAt: 9})
		if err != nil {
			t.Fatal(err)
		}
		if id3 != id1 {
			t.Fatalf("upsert changed id: got %d, want %d", id3, id1)
		}
		all, err := st.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 2 {
			t.Fatalf("List len = %d, want 2", len(all))
		}
		if all[0].Name != "a" || all[0].CronExpr != "30 4 * * *" ||
			all[0].Action != "x2" || !all[0].Enabled || all[0].NextFireAt != 300 {
			t.Fatalf("upsert did not replace fields: %+v", all[0])
		}
	})

	t.Run("list sorted by name", func(t *testing.T) {
		st := newStore(t)
		for _, n := range []string{"c", "a", "b"} {
			if _, err := st.Add(ctx, cronsched.Schedule{Name: n, Enabled: true, NextFireAt: 1, CreatedAt: 1}); err != nil {
				t.Fatal(err)
			}
		}
		all, _ := st.List(ctx)
		if len(all) != 3 || all[0].Name != "a" || all[1].Name != "b" || all[2].Name != "c" {
			t.Fatalf("List not name-sorted: %+v", all)
		}
	})

	t.Run("due filters and orders", func(t *testing.T) {
		st := newStore(t)
		seed := []cronsched.Schedule{
			{Name: "due-late", Enabled: true, NextFireAt: 100, CreatedAt: 1},
			{Name: "due-early", Enabled: true, NextFireAt: 50, CreatedAt: 1},
			{Name: "disabled", Enabled: false, NextFireAt: 30, CreatedAt: 1},
			{Name: "unarmed", Enabled: true, NextFireAt: 0, CreatedAt: 1},
			{Name: "future", Enabled: true, NextFireAt: 900, CreatedAt: 1},
		}
		for _, s := range seed {
			if _, err := st.Add(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
		due, err := st.Due(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(due) != 2 || due[0].Name != "due-early" || due[1].Name != "due-late" {
			t.Fatalf("Due = %+v, want [due-early due-late] (oldest first, enabled+armed only)", due)
		}
	})

	t.Run("record fire advances and audits", func(t *testing.T) {
		st := newStore(t)
		id, _ := st.Add(ctx, cronsched.Schedule{Name: "j", CronExpr: "0 2 * * *",
			Action: "act", Enabled: true, NextFireAt: 100, CreatedAt: 1})
		if err := st.RecordFire(ctx, id, "ok", 1.5, 999); err != nil {
			t.Fatal(err)
		}
		all, _ := st.List(ctx)
		e := all[0]
		if e.LastFiredAt <= 0 || e.LastResult != "ok" || e.NextFireAt != 999 || !e.Enabled {
			t.Fatalf("chain not advanced: %+v", e)
		}
		events, err := st.RecentEvents(ctx, 10)
		if err != nil || len(events) != 1 {
			t.Fatalf("RecentEvents = %+v, err %v", events, err)
		}
		ev := events[0]
		if ev.ScheduleID != id || ev.Name != "j" || ev.Action != "act" ||
			ev.Result != "ok" || ev.DurationSeconds != 1.5 {
			t.Fatalf("audit row = %+v", ev)
		}
	})

	t.Run("record fire disables exhausted chain", func(t *testing.T) {
		st := newStore(t)
		id, _ := st.Add(ctx, cronsched.Schedule{Name: "dead", CronExpr: "0 2 * * *",
			Action: "act", Enabled: true, NextFireAt: 100, CreatedAt: 1})
		if err := st.RecordFire(ctx, id, "ran; disabled: cron has no future fire time", 0, 0); err != nil {
			t.Fatal(err)
		}
		all, _ := st.List(ctx)
		if all[0].Enabled {
			t.Fatal("exhausted chain still enabled")
		}
		due, _ := st.Due(ctx, 1<<40)
		if len(due) != 0 {
			t.Fatalf("disabled schedule came back due: %+v", due)
		}
		if events, _ := st.RecentEvents(ctx, 10); len(events) != 1 {
			t.Fatalf("exhaustion stamp must still audit, got %+v", events)
		}
	})

	t.Run("trigger now arms enabled only", func(t *testing.T) {
		st := newStore(t)
		if _, err := st.Add(ctx, cronsched.Schedule{Name: "on", Enabled: true, NextFireAt: 99999, CreatedAt: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Add(ctx, cronsched.Schedule{Name: "off", Enabled: false, NextFireAt: 99999, CreatedAt: 1}); err != nil {
			t.Fatal(err)
		}
		if err := st.TriggerNow(ctx, "on"); err != nil {
			t.Fatal(err)
		}
		if err := st.TriggerNow(ctx, "off"); !errors.Is(err, cronsched.ErrNotFound) {
			t.Fatalf("disabled trigger err = %v, want ErrNotFound", err)
		}
		if err := st.TriggerNow(ctx, "nope"); !errors.Is(err, cronsched.ErrNotFound) {
			t.Fatalf("unknown trigger err = %v, want ErrNotFound", err)
		}
		// Millisecond-precision now, taken AFTER the stamp: a truncated
		// whole-second now could read as "not yet due" within the stamp's
		// own second.
		due, _ := st.Due(ctx, float64(time.Now().UnixMilli())/1000.0)
		if len(due) != 1 || due[0].Name != "on" {
			t.Fatalf("triggered schedule not due: %+v", due)
		}
	})

	t.Run("set enabled toggles", func(t *testing.T) {
		st := newStore(t)
		if _, err := st.Add(ctx, cronsched.Schedule{Name: "s", Enabled: true, NextFireAt: 1, CreatedAt: 1}); err != nil {
			t.Fatal(err)
		}
		if err := st.SetEnabled(ctx, "s", false); err != nil {
			t.Fatal(err)
		}
		if due, _ := st.Due(ctx, 1<<40); len(due) != 0 {
			t.Fatalf("disabled schedule due: %+v", due)
		}
		if err := st.SetEnabled(ctx, "s", true); err != nil {
			t.Fatal(err)
		}
		if err := st.SetEnabled(ctx, "nope", true); !errors.Is(err, cronsched.ErrNotFound) {
			t.Fatalf("unknown toggle err = %v, want ErrNotFound", err)
		}
	})

	t.Run("remove", func(t *testing.T) {
		st := newStore(t)
		if _, err := st.Add(ctx, cronsched.Schedule{Name: "gone", Enabled: true, NextFireAt: 1, CreatedAt: 1}); err != nil {
			t.Fatal(err)
		}
		if err := st.Remove(ctx, "gone"); err != nil {
			t.Fatal(err)
		}
		if err := st.Remove(ctx, "gone"); !errors.Is(err, cronsched.ErrNotFound) {
			t.Fatalf("double remove err = %v, want ErrNotFound", err)
		}
	})

	t.Run("earliest", func(t *testing.T) {
		st := newStore(t)
		if _, ok := st.Earliest(ctx); ok {
			t.Fatal("empty store reported an earliest fire")
		}
		for _, s := range []cronsched.Schedule{
			{Name: "e2", Enabled: true, NextFireAt: 200, CreatedAt: 1},
			{Name: "e1", Enabled: true, NextFireAt: 100, CreatedAt: 1},
			{Name: "e3", Enabled: false, NextFireAt: 50, CreatedAt: 1}, // ignored: disabled
		} {
			if _, err := st.Add(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
		at, ok := st.Earliest(ctx)
		if !ok || at != 100 {
			t.Fatalf("Earliest = (%v, %v), want (100, true)", at, ok)
		}
	})

	t.Run("recent events newest first with limit", func(t *testing.T) {
		st := newStore(t)
		var ids []int64
		for _, n := range []string{"r1", "r2", "r3"} {
			id, err := st.Add(ctx, cronsched.Schedule{Name: n, Enabled: true, NextFireAt: 1, CreatedAt: 1})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		for _, id := range ids {
			time.Sleep(2 * time.Millisecond) // distinct fired_at ordering
			if err := st.RecordFire(ctx, id, "run", 0, 2); err != nil {
				t.Fatal(err)
			}
		}
		events, err := st.RecentEvents(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 {
			t.Fatalf("limit ignored: %d events", len(events))
		}
		if events[0].Name != "r3" || events[1].Name != "r2" {
			t.Fatalf("not newest first: %s then %s", events[0].Name, events[1].Name)
		}
	})
}
