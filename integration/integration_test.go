// Package integration runs the cronsched Store contract suite against real
// MySQL and Postgres servers. It lives in its own module so the drivers
// never land in the library's dependency tree.
//
// Enabled by environment variables (skipped when unset):
//
//	CRONSCHED_MYSQL_DSN     e.g. root:root@tcp(127.0.0.1:3306)/cronsched_test
//	CRONSCHED_POSTGRES_DSN  e.g. postgres://postgres:postgres@127.0.0.1:5432/cronsched_test?sslmode=disable
package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sun-praise/cronsched"
	"github.com/sun-praise/cronsched/storetest"
	_ "github.com/lib/pq"
)

func TestMySQLStoreContract(t *testing.T) {
	dsn := os.Getenv("CRONSCHED_MYSQL_DSN")
	if dsn == "" {
		t.Skip("CRONSCHED_MYSQL_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st := cronsched.NewSQLStore(db)

	// Fresh tables per run.
	drop := func(t *testing.T) {
		t.Helper()
		for _, stmt := range []string{
			`DROP TABLE IF EXISTS schedule_events`,
			`DROP TABLE IF EXISTS schedules`,
		} {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.Migrate(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	storetest.Contract(t, func(t *testing.T) cronsched.Store {
		drop(t)
		return st
	})
}

func TestPostgresStoreContract(t *testing.T) {
	dsn := os.Getenv("CRONSCHED_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CRONSCHED_POSTGRES_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st := cronsched.NewSQLStore(db)

	// Fresh tables per run.
	drop := func(t *testing.T) {
		t.Helper()
		for _, stmt := range []string{
			`DROP TABLE IF EXISTS schedule_events`,
			`DROP TABLE IF EXISTS schedules`,
		} {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.Migrate(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	storetest.Contract(t, func(t *testing.T) cronsched.Store {
		drop(t)
		return st
	})
}
