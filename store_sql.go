package cronsched

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SQLStore is the Store implementation for any database/sql backend:
// SQLite (including a database file your program already owns), MySQL and
// Postgres. Dialects shape the DDL, the upsert and the placeholders;
// everything else is plain SQL.
//
// Construction is infallible: NewSQLStore never fails. Dialect detection
// and schema creation happen lazily on first use — an undetectable driver
// makes every operation return a clear error instead of failing the
// constructor (see WithSQLStore for the option form).
type SQLStore struct {
	db *sql.DB
	// dialect is pinned by the constructor options or by driver-name
	// detection on first use; "" means "detect lazily". Every method goes
	// through ready() which pins it once.
	dialect    SQLDialect

	ensureOnce sync.Once
	ensureErr  error
}

// SQLOption configures a SQLStore.
type SQLOption func(*SQLStore)

// WithDialect overrides driver-name detection. Use it when the driver name
// is ambiguous (e.g. a proxy driver) — without it, an unguessable driver
// makes every store operation fail with a clear error.
func WithDialect(d SQLDialect) SQLOption { return func(s *SQLStore) { s.dialect = d } }

// NewSQLStore builds a SQLStore on an existing *sql.DB — typically the
// handle your program already uses. The dialect is detected from the
// driver name and can be overridden with WithDialect. Tables are created
// on first use; call Migrate to create them explicitly.
func NewSQLStore(db *sql.DB, opts ...SQLOption) *SQLStore {
	s := &SQLStore{db: db}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Close releases the underlying database handle. Only call it when your
// program is done with the handle — when the handle is shared with the
// rest of your application, closing is the application's job.
func (s *SQLStore) Close() error { return s.db.Close() }

// Migrate creates the schedules and schedule_events tables (no-op if they
// exist). Run calls it lazily; call it explicitly when you want the schema
// ready before the first tick.
func (s *SQLStore) Migrate(ctx context.Context) error {
	return s.ready(ctx)
}

// detectDialect probes the server: SELECT sqlite_version() is definitive
// for SQLite; version() content separates Postgres ("PostgreSQL ...")
// from MySQL/MariaDB (a bare semver). A pure probe approach needs no
// driver-specific imports and works through proxy drivers too.
func detectDialect(db *sql.DB) (SQLDialect, error) {
	var probe any
	if err := db.QueryRow(`SELECT sqlite_version()`).Scan(&probe); err == nil {
		return DialectSQLite, nil
	}
	var v string
	if err := db.QueryRow(`SELECT version()`).Scan(&v); err != nil {
		return "", fmt.Errorf("cronsched: cannot probe database version for dialect detection: %w", err)
	}
	if strings.Contains(strings.ToLower(v), "postgre") {
		return DialectPostgres, nil
	}
	if v != "" && v[0] >= '0' && v[0] <= '9' {
		return DialectMySQL, nil // bare semver: MySQL/MariaDB convention
	}
	return "", fmt.Errorf("cronsched: unrecognised version() %q — set the dialect explicitly with WithDialect", v)
}

// ready pins the dialect (once) and creates the schema (once). The error
// is sticky: after a failure every operation re-reports it.
func (s *SQLStore) ready(ctx context.Context) error {
	s.ensureOnce.Do(func() {
		if s.dialect == "" {
			d, err := detectDialect(s.db)
			if err != nil {
				s.ensureErr = err
				return
			}
			s.dialect = d
		}
		for _, stmt := range s.dialect.createStmts() {
			if _, err := s.db.ExecContext(ctx, s.dialect.bindSQL(stmt)); err != nil {
				s.ensureErr = fmt.Errorf("cronsched: migrate: %w", err)
				return
			}
		}
	})
	return s.ensureErr
}

const sqlScheduleCols = `id, name, cron_expr, action, params, enabled,
	COALESCE(next_fire_at,0), COALESCE(last_fired_at,0), last_result, created_at`

func scanSQLSchedule(row interface{ Scan(...any) error }) (Schedule, error) {
	var s Schedule
	var enabled int
	if err := row.Scan(&s.ID, &s.Name, &s.CronExpr, &s.Action, &s.Params,
		&enabled, &s.NextFireAt, &s.LastFiredAt, &s.LastResult, &s.CreatedAt); err != nil {
		return Schedule{}, err
	}
	s.Enabled = enabled != 0
	return s, nil
}

// Add upserts by name (see the Store contract).
func (s *SQLStore) Add(ctx context.Context, n Schedule) (int64, error) {
	if err := s.ready(ctx); err != nil {
		return 0, err
	}
	if _, err := s.db.ExecContext(ctx, s.dialect.bindSQL(s.dialect.upsertStmt()),
		n.Name, n.CronExpr, n.Action, n.Params, n.Enabled, n.NextFireAt, n.CreatedAt); err != nil {
		return 0, fmt.Errorf("cronsched: schedule add %q: %w", n.Name, err)
	}
	// LastInsertId is unreliable on the UPDATE arm of the upsert (driver
	// dependent) — read the row's id back by its unique name.
	var id int64
	if err := s.db.QueryRowContext(ctx, s.dialect.bindSQL(
		`SELECT id FROM schedules WHERE name = ?`), n.Name).Scan(&id); err != nil {
		return 0, fmt.Errorf("cronsched: schedule add %q: read back id: %w", n.Name, err)
	}
	return id, nil
}

// List returns all schedules sorted by name.
func (s *SQLStore) List(ctx context.Context) ([]Schedule, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.dialect.bindSQL(
		`SELECT `+sqlScheduleCols+` FROM schedules ORDER BY name`))
	if err != nil {
		return nil, fmt.Errorf("cronsched: schedule list: %w", err)
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		e, err := scanSQLSchedule(rows)
		if err != nil {
			return nil, fmt.Errorf("cronsched: scan schedule: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Remove drops one schedule by name.
func (s *SQLStore) Remove(ctx context.Context, name string) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, s.dialect.bindSQL(
		`DELETE FROM schedules WHERE name = ?`), name)
	if err != nil {
		return fmt.Errorf("cronsched: schedule remove %q: %w", name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	return nil
}

// SetEnabled toggles a schedule without losing its fire chain.
func (s *SQLStore) SetEnabled(ctx context.Context, name string, enabled bool) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	v := 0
	if enabled {
		v = 1
	}
	res, err := s.db.ExecContext(ctx, s.dialect.bindSQL(
		`UPDATE schedules SET enabled = ? WHERE name = ?`), v, name)
	if err != nil {
		return fmt.Errorf("cronsched: schedule toggle %q: %w", name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	return nil
}

// Due returns enabled, armed schedules whose NextFireAt has passed, oldest
// due first.
func (s *SQLStore) Due(ctx context.Context, now float64) ([]Schedule, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.dialect.bindSQL(`
		SELECT `+sqlScheduleCols+` FROM schedules
		WHERE enabled = 1 AND next_fire_at IS NOT NULL AND next_fire_at > 0 AND next_fire_at <= ?
		ORDER BY next_fire_at`), now)
	if err != nil {
		return nil, fmt.Errorf("cronsched: due schedules: %w", err)
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		e, err := scanSQLSchedule(rows)
		if err != nil {
			return nil, fmt.Errorf("cronsched: scan due schedule: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// RecordFire stamps the fire outcome and appends the audit row. A
// nextFireAt > 0 advances the chain; <= 0 disables the schedule (chain
// exhausted). The writes run on a detached context: an audit row must land
// even when the caller's context is cancelled mid-fire (shutdown race).
func (s *SQLStore) RecordFire(ctx context.Context, id int64, result string, tookSeconds float64, nextFireAt float64) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ts := unixSeconds(time.Now())
	if nextFireAt > 0 {
		if _, err := s.db.ExecContext(wctx, s.dialect.bindSQL(`
			UPDATE schedules SET last_fired_at = ?, last_result = ?, next_fire_at = ?
			WHERE id = ?`), ts, result, nextFireAt, id); err != nil {
			return fmt.Errorf("cronsched: stamp fire %d: %w", id, err)
		}
	} else if _, err := s.db.ExecContext(wctx, s.dialect.bindSQL(`
		UPDATE schedules SET last_fired_at = ?, last_result = ?, enabled = 0
		WHERE id = ?`), ts, result, id); err != nil {
		return fmt.Errorf("cronsched: stamp fire %d: %w", id, err)
	}
	if _, err := s.db.ExecContext(wctx, s.dialect.bindSQL(`
		INSERT INTO schedule_events (schedule_id, name, fired_at, action, result, duration_seconds)
		SELECT id, name, ?, action, ?, ? FROM schedules WHERE id = ?`),
		ts, result, tookSeconds, id); err != nil {
		return fmt.Errorf("cronsched: audit fire %d: %w", id, err)
	}
	return nil
}

// TriggerNow arms a schedule to fire on the next Due query.
func (s *SQLStore) TriggerNow(ctx context.Context, name string) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, s.dialect.bindSQL(
		`UPDATE schedules SET next_fire_at = ? WHERE name = ? AND enabled = 1`),
		unixSeconds(time.Now()), name)
	if err != nil {
		return fmt.Errorf("cronsched: trigger %q: %w", name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w or disabled: %q", ErrNotFound, name)
	}
	return nil
}

// RecentEvents returns the audit tail, newest first.
func (s *SQLStore) RecentEvents(ctx context.Context, limit int) ([]Event, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, s.dialect.bindSQL(`
		SELECT id, schedule_id, name, fired_at, action, result, duration_seconds
		FROM schedule_events ORDER BY fired_at DESC LIMIT ?`), limit)
	if err != nil {
		return nil, fmt.Errorf("cronsched: recent schedule events: %w", err)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.ScheduleID, &e.Name, &e.FiredAt,
			&e.Action, &e.Result, &e.DurationSeconds); err != nil {
			return nil, fmt.Errorf("cronsched: scan schedule event: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Earliest returns the soonest armed NextFireAt, if any.
func (s *SQLStore) Earliest(ctx context.Context) (float64, bool) {
	if err := s.ready(ctx); err != nil {
		return 0, false
	}
	var at float64
	err := s.db.QueryRowContext(ctx, s.dialect.bindSQL(
		`SELECT MIN(next_fire_at) FROM schedules WHERE enabled = 1 AND next_fire_at IS NOT NULL`)).Scan(&at)
	if err != nil || at <= 0 {
		return 0, false
	}
	return at, true
}
