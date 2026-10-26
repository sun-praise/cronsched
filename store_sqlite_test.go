package cronsched_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sun-praise/cronsched"
	"github.com/sun-praise/cronsched/storetest"
)

func TestSQLiteStoreContract(t *testing.T) {
	storetest.Contract(t, func(t *testing.T) cronsched.Store {
		st, err := cronsched.OpenSQLiteStore(context.Background(),
			filepath.Join(t.TempDir(), "sched.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	})
}

// TestSQLiteStoreReopensWithState pins the persistence property the
// library exists for: schedules and the audit ledger survive close/open.
func TestSQLiteStoreReopensWithState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sched.db")

	st, err := cronsched.OpenSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Add(ctx, cronsched.Schedule{Name: "nightly", CronExpr: "0 2 * * *",
		Action: "backup", Params: `{}`, Enabled: true, NextFireAt: 100, CreatedAt: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordFire(ctx, 1, "ok", 0.5, 999); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := cronsched.OpenSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	all, err := reopened.List(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("reopened List = %+v, err %v", all, err)
	}
	if all[0].NextFireAt != 999 || all[0].LastResult != "ok" {
		t.Fatalf("fire chain lost across reopen: %+v", all[0])
	}
	if events, _ := reopened.RecentEvents(ctx, 10); len(events) != 1 {
		t.Fatalf("audit ledger lost across reopen: %+v", events)
	}
}
