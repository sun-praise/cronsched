package cronsched

import (
	"context"
	"database/sql"
	"fmt"

	// Pure-Go SQLite (no CGO). The driver registers itself as "sqlite".
	_ "modernc.org/sqlite"
)

// SQLiteStore is a SQLStore bound to a SQLite database — the whole
// scheduler state in one portable file, no CGO and no external services.
// It is an alias: everything documented for SQLStore applies.
type SQLiteStore = SQLStore

// OpenSQLiteStore opens (creating if necessary) the database file and
// ensures the schema exists.
func OpenSQLiteStore(ctx context.Context, path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("cronsched: open sqlite %q: %w", path, err)
	}
	// SQLite is single-writer; one connection avoids SQLITE_BUSY churn
	// under concurrent ticks.
	db.SetMaxOpenConns(1)
	s := &SQLiteStore{db: db, dialect: DialectSQLite}
	if err := s.Migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// NewSQLiteStoreFromDB binds a SQLite store to an existing *sql.DB — the
// handle your program already owns (the schema is created lazily on first
// use; call Migrate to force it). Because the schema matches
// sun-praise/ralphplus's schedules tables, an existing ralphplus state
// database can be opened as-is.
func NewSQLiteStoreFromDB(db *sql.DB) *SQLiteStore {
	return &SQLStore{db: db, dialect: DialectSQLite}
}
