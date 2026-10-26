package cronsched_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sun-praise/cronsched"
	"github.com/sun-praise/cronsched/storetest"
)

// fakeDriver registers a driver whose queries fail cleanly, to pin the
// construction contract: NewSQLStore/WithSQLStore never fail at build
// time; problems surface as operation errors.
type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("not a real db") }

var fakeDriverSeq atomic.Int64

// TestSQLStoreConstructionIsInfallible registers a driver whose queries
// fail cleanly, to pin the construction contract: NewSQLStore/WithSQLStore
// never fail at build time; problems surface as operation errors.
func TestSQLStoreConstructionIsInfallible(t *testing.T) {
	// Unique name per invocation — sql.Register panics on duplicates
	// (go test -count=2 runs the test twice in one process).
	driverName := fmt.Sprintf("crontest-fake-%d", fakeDriverSeq.Add(1))
	sql.Register(driverName, fakeDriver{})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// WithSQLStore wiring with a driver that cannot serve queries: New
	// succeeds, operations report the failure.
	s := cronsched.New(cronsched.WithSQLStore(db))
	if _, err := s.List(context.Background()); err == nil {
		t.Fatal("List succeeded on a driver that cannot serve queries")
	}
}

// TestSQLStoreContractSQLite runs the shared contract suite against the
// generic SQLStore on SQLite — both the auto-detected (probe) and
// explicit-dialect/FromDB forms.
func TestSQLStoreContractSQLite(t *testing.T) {
	ctx := context.Background()

	// The *sql.DB is shared across subtests, so each subtest provisions
	// fresh state by dropping and re-migrating the tables.
	variants := []struct {
		name  string
		build func(db *sql.DB) *cronsched.SQLStore
	}{
		{"autodetect via probe", func(db *sql.DB) *cronsched.SQLStore {
			return cronsched.NewSQLStore(db)
		}},
		{"explicit dialect", func(db *sql.DB) *cronsched.SQLStore {
			return cronsched.NewSQLStore(db, cronsched.WithDialect(cronsched.DialectSQLite))
		}},
		{"from db", func(db *sql.DB) *cronsched.SQLStore {
			return cronsched.NewSQLiteStoreFromDB(db)
		}},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sched.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			// A fresh store instance re-runs the lazy migrate; the DROPs
			// guarantee the schema is rebuilt from zero, rows included.
			fresh := func(t *testing.T) cronsched.Store {
				t.Helper()
				for _, stmt := range []string{
					`DROP TABLE IF EXISTS schedule_events`,
					`DROP TABLE IF EXISTS schedules`,
				} {
					if _, err := db.Exec(stmt); err != nil {
						t.Fatal(err)
					}
				}
				return v.build(db)
			}
			storetest.Contract(t, fresh)
		})
	}

	t.Run("migrate explicit before use", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "sched.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		st := cronsched.NewSQLStore(db)
		if err := st.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Add(ctx, cronsched.Schedule{Name: "m", Enabled: true, NextFireAt: 1, CreatedAt: 1}); err != nil {
			t.Fatal(err)
		}
	})
}

// TestWithSQLStoreWiresTheScheduler pins the integration story: one option
// over an existing handle, scheduler fully operational.
func TestWithSQLStoreWiresTheScheduler(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sched.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var now atomic.Value
	now.Store(time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)) // 01:00 — before the 02:00 slot
	var fired bool
	s := cronsched.New(
		cronsched.WithSQLStore(db),
		cronsched.WithClock(func() time.Time { return now.Load().(time.Time) }),
		cronsched.WithHandler(func(ctx context.Context, action, params string) (string, error) {
			fired = true
			return "ok", nil
		}),
	)
	if _, err := s.Add(context.Background(), "j", "0 2 * * *", "x", ""); err != nil {
		t.Fatal(err)
	}
	now.Store(time.Date(2026, 10, 18, 3, 0, 0, 0, time.UTC)) // cross the slot
	if n := s.Tick(context.Background()); n != 1 {
		t.Fatalf("Tick processed %d entries, want 1", n)
	}
	if !fired {
		t.Fatal("handler never ran")
	}
}
