package cronsched

import (
	"fmt"
	"strings"
)

// SQLDialect selects the SQL variant a SQLStore speaks.
type SQLDialect string

const (
	DialectSQLite   SQLDialect = "sqlite"
	DialectMySQL    SQLDialect = "mysql"
	DialectPostgres SQLDialect = "postgres"
)

// bindSQL rewrites a query written with "?" placeholders into the dialect's
// placeholder form: positional $n for Postgres, "?" as-is elsewhere.
// Every query string in this package is a constant with placeholders only
// in argument positions, so an in-order rewrite is always correct.
func (d SQLDialect) bindSQL(query string) string {
	if d != DialectPostgres {
		return query
	}
	var b strings.Builder
	n := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			n++
			b.WriteString("$")
			b.WriteString(fmt.Sprint(n))
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

// createStmts returns the per-dialect DDL. MySQL uses ON DUPLICATE KEY
// (accepted by every 5.7/8.x/MariaDB); SQLite and Postgres share the
// standard ON CONFLICT upsert.
func (d SQLDialect) createStmts() []string {
	switch d {
	case DialectMySQL:
		return []string{
			`CREATE TABLE IF NOT EXISTS schedules (
				id BIGINT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL UNIQUE,
				cron_expr VARCHAR(255) NOT NULL,
				action VARCHAR(255) NOT NULL,
				params TEXT NOT NULL,
				enabled TINYINT NOT NULL DEFAULT 1,
				next_fire_at DOUBLE NULL,
				last_fired_at DOUBLE NULL,
				last_result TEXT NULL,
				created_at DOUBLE NOT NULL,
				INDEX idx_schedules_due (enabled, next_fire_at)
			) ENGINE=InnoDB`,
			`CREATE TABLE IF NOT EXISTS schedule_events (
				id BIGINT AUTO_INCREMENT PRIMARY KEY,
				schedule_id BIGINT NOT NULL,
				name VARCHAR(255) NOT NULL,
				fired_at DOUBLE NOT NULL,
				action VARCHAR(255) NOT NULL,
				result TEXT NOT NULL,
				duration_seconds DOUBLE NOT NULL DEFAULT 0
			)`,
		}
	case DialectPostgres:
		return []string{
			`CREATE TABLE IF NOT EXISTS schedules (
				id BIGSERIAL PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				cron_expr TEXT NOT NULL,
				action TEXT NOT NULL,
				params TEXT NOT NULL DEFAULT '',
				enabled SMALLINT NOT NULL DEFAULT 1,
				next_fire_at DOUBLE PRECISION,
				last_fired_at DOUBLE PRECISION,
				last_result TEXT NOT NULL DEFAULT '',
				created_at DOUBLE PRECISION NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, next_fire_at)`,
			`CREATE TABLE IF NOT EXISTS schedule_events (
				id BIGSERIAL PRIMARY KEY,
				schedule_id BIGINT NOT NULL,
				name TEXT NOT NULL,
				fired_at DOUBLE PRECISION NOT NULL,
				action TEXT NOT NULL,
				result TEXT NOT NULL DEFAULT '',
				duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0
			)`,
		}
	default: // sqlite
		return []string{
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
		}
	}
}

// upsertStmt returns the schedules upsert for the dialect.
func (d SQLDialect) upsertStmt() string {
	const cols = `name, cron_expr, action, params, enabled, next_fire_at, created_at`
	switch d {
	case DialectMySQL:
		return `INSERT INTO schedules (` + cols + `)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				cron_expr = VALUES(cron_expr), action = VALUES(action),
				params = VALUES(params), enabled = VALUES(enabled),
				next_fire_at = VALUES(next_fire_at)`
	default:
		return `INSERT INTO schedules (` + cols + `)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(name) DO UPDATE SET
				cron_expr = excluded.cron_expr, action = excluded.action,
				params = excluded.params, enabled = excluded.enabled,
				next_fire_at = excluded.next_fire_at`
	}
}
