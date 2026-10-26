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
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/sun-praise/cronsched"
	"github.com/sun-praise/cronsched/storetest"
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
	// Fresh tables per subtest: drop, then a NEW store instance — its
	// lazy migrate rebuilds the schema (a reused instance's ensure-once
	// would skip it).
	drop := func(t *testing.T) cronsched.Store {
		t.Helper()
		for _, stmt := range []string{
			`DROP TABLE IF EXISTS schedule_events`,
			`DROP TABLE IF EXISTS schedules`,
		} {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
		return cronsched.NewSQLStore(db)
	}
	storetest.Contract(t, drop)
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
	drop := func(t *testing.T) cronsched.Store {
		t.Helper()
		for _, stmt := range []string{
			`DROP TABLE IF EXISTS schedule_events`,
			`DROP TABLE IF EXISTS schedules`,
		} {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
		return cronsched.NewSQLStore(db)
	}
	storetest.Contract(t, drop)
}
