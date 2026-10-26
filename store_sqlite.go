package cronsched

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// Pure-Go SQLite (no CGO). The driver registers itself as "sqlite".
	_ "modernc.org/sqlite"
)

// SQLiteStore is a Store backed by a SQLite database file — the whole
// scheduler state in one portable file, no CGO and no external services.
//
// The schema is two plain tables (schedules, schedule_events) created on
// open if absent; it intentionally matches the column layout used by
// sun-praise/ralphplus, so an existing database file can be opened as-is.
//
// All handles share one connection (SQLite is single-writer), and
// RecordFire deliberately runs on a detached context with its own timeout:
// an audit row must land even when the caller's context is being cancelled
// mid-fire (shutdown race).
type SQLiteStore struct {
	db *sql.DB
}

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
	s := &SQLiteStore{db: db}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS schedules (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			cron_expr TEXT NOT NULL,
			action TEXT NOT NULL,
			params TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1,
			next_fire_at REAL,
			last_fired_at REAL,
			last_result TEXT NOT NULL DEFAULT '',
			created_at REAL NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, next_fire_at)`,
		`CREATE TABLE IF NOT EXISTS schedule_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			schedule_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			fired_at REAL NOT NULL,
			action TEXT NOT NULL,
			result TEXT NOT NULL DEFAULT '',
			duration_seconds REAL NOT NULL DEFAULT 0
		)`,
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("cronsched: migrate: %w", err)
		}
	}
	return nil
}

// Close releases the database handle.
func (s *SQLiteStore) Close() error { return s.db.Close() }

const scheduleCols = `id, name, cron_expr, action, params, enabled,
	COALESCE(next_fire_at,0), COALESCE(last_fired_at,0), last_result, created_at`

func scanSchedule(row interface{ Scan(...any) error }) (Schedule, error) {
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
func (s *SQLiteStore) Add(ctx context.Context, n Schedule) (int64, error) {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO schedules (name, cron_expr, action, params, enabled, next_fire_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			cron_expr = excluded.cron_expr, action = excluded.action,
			params = excluded.params, enabled = excluded.enabled,
			next_fire_at = excluded.next_fire_at`,
		n.Name, n.CronExpr, n.Action, n.Params, n.Enabled, n.NextFireAt, n.CreatedAt); err != nil {
		return 0, fmt.Errorf("cronsched: schedule add %q: %w", n.Name, err)
	}
	// LastInsertId is unreliable on the UPDATE arm of the upsert — read
	// the row's id back by its unique name.
	var id int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT id FROM schedules WHERE name = ?`, n.Name).Scan(&id); err != nil {
		return 0, fmt.Errorf("cronsched: schedule add %q: read back id: %w", n.Name, err)
	}
	return id, nil
}

// List returns all schedules sorted by name.
func (s *SQLiteStore) List(ctx context.Context) ([]Schedule, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+scheduleCols+` FROM schedules ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("cronsched: schedule list: %w", err)
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		e, err := scanSchedule(rows)
		if err != nil {
			return nil, fmt.Errorf("cronsched: scan schedule: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Remove drops one schedule by name.
func (s *SQLiteStore) Remove(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM schedules WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("cronsched: schedule remove %q: %w", name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	return nil
}

// SetEnabled toggles a schedule without losing its fire chain.
func (s *SQLiteStore) SetEnabled(ctx context.Context, name string, enabled bool) error {
	v := 0
	if enabled {
		v = 1
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE schedules SET enabled = ? WHERE name = ?`, v, name)
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
func (s *SQLiteStore) Due(ctx context.Context, now float64) ([]Schedule, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+scheduleCols+` FROM schedules
		WHERE enabled = 1 AND next_fire_at IS NOT NULL AND next_fire_at > 0 AND next_fire_at <= ?
		ORDER BY next_fire_at`, now)
	if err != nil {
		return nil, fmt.Errorf("cronsched: due schedules: %w", err)
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		e, err := scanSchedule(rows)
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
func (s *SQLiteStore) RecordFire(ctx context.Context, id int64, result string, tookSeconds float64, nextFireAt float64) error {
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ts := unixSeconds(time.Now())
	if nextFireAt > 0 {
		if _, err := s.db.ExecContext(wctx, `
			UPDATE schedules SET last_fired_at = ?, last_result = ?, next_fire_at = ?
			WHERE id = ?`, ts, result, nextFireAt, id); err != nil {
			return fmt.Errorf("cronsched: stamp fire %d: %w", id, err)
		}
	} else if _, err := s.db.ExecContext(wctx, `
		UPDATE schedules SET last_fired_at = ?, last_result = ?, enabled = 0
		WHERE id = ?`, ts, result, id); err != nil {
		return fmt.Errorf("cronsched: stamp fire %d: %w", id, err)
	}
	if _, err := s.db.ExecContext(wctx, `
		INSERT INTO schedule_events (schedule_id, name, fired_at, action, result, duration_seconds)
		SELECT id, name, ?, action, ?, ? FROM schedules WHERE id = ?`,
		ts, result, tookSeconds, id); err != nil {
		return fmt.Errorf("cronsched: audit fire %d: %w", id, err)
	}
	return nil
}

// TriggerNow arms a schedule to fire on the next Due query.
func (s *SQLiteStore) TriggerNow(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE schedules SET next_fire_at = ? WHERE name = ? AND enabled = 1`,
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
func (s *SQLiteStore) RecentEvents(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, schedule_id, name, fired_at, action, result, duration_seconds
		FROM schedule_events ORDER BY fired_at DESC LIMIT ?`, limit)
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
func (s *SQLiteStore) Earliest(ctx context.Context) (float64, bool) {
	var at float64
	err := s.db.QueryRowContext(ctx,
		`SELECT MIN(next_fire_at) FROM schedules WHERE enabled = 1 AND next_fire_at IS NOT NULL`).Scan(&at)
	if err != nil || at <= 0 {
		return 0, false
	}
	return at, true
}
