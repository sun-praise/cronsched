// Package cronexpr parses cron expressions behind a small Expr API.
//
// The parser is robfig/cron/v3's standard parser: five fields (minute hour
// dom mon dow), lists/ranges/steps, named months/weekdays (MON-FRI),
// @ descriptors (@hourly/@daily/@weekly/@monthly/@yearly/@every) and
// CRON_TZ= prefixes. On top of it, this package folds Vixie's Sunday alias
// "7" in the day-of-week field (the standard parser tops out at 6) and
// rejects @every below one minute (a deliberate anti-tight-loop floor).
package cronexpr

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// parser is the standard 5-field spec (min hour dom mon dow), second-free,
// plus @ descriptors (@hourly/@daily/@every/...).
var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Expr is a parsed cron expression.
type Expr struct {
	sched cron.Schedule
	raw   string
}

// Parse compiles a cron expression. Accepts the full standard vocabulary:
// lists, ranges, steps, named months/weekdays (MON-FRI), @ descriptors
// (@hourly/@daily/@weekly/@monthly/@yearly/@every) and CRON_TZ= prefixes.
//
// @every below one minute is rejected: scheduled at sub-minute cadence, a
// fast-wake loop degenerates into a per-minute batch at best and a hot
// spin at worst. Higher cadence is a ticker's job, not a cron's.
func Parse(s string) (*Expr, error) {
	trimmed := strings.TrimSpace(s)
	if fs := strings.Fields(trimmed); len(fs) >= 2 && fs[0] == "@every" {
		if d, err := time.ParseDuration(fs[1]); err == nil && d < time.Minute {
			return nil, fmt.Errorf("cron %q: @every below 1m is rejected (anti-tight-loop floor)", s)
		}
	}
	sched, err := parser.Parse(normalizeDowSeven(s))
	if err != nil {
		return nil, fmt.Errorf("cron %q: %w", s, err)
	}
	return &Expr{sched: sched, raw: s}, nil
}

// Next returns the next fire time strictly after t, in the expression's
// location (CRON_TZ-aware). The zero time means "no future fire within the
// parser's give-up window" — e.g. an impossible date like Feb 30. Callers
// must treat the zero time as "never fires again", not as an error.
func (e *Expr) Next(t time.Time) time.Time {
	return e.sched.Next(t)
}

// String returns the raw expression.
func (e *Expr) String() string { return e.raw }

// normalizeDowSeven folds Vixie's Sunday alias "7" into "0" in the
// day-of-week field — the standard parser tops out at 6. Token-level
// (never a blanket digit replace, which mangles ranges like 5-7):
// "7"→"0" (single values don't expand), "a-7"→"a-6,0", "a-7/s"→"a-6/s"
// (",0" when the step lands on Sunday), stepped stars ("*/7") pass
// through — the parser already reads them as {0}. CRON_TZ prefixes are
// stripped for the fold and re-attached.
func normalizeDowSeven(spec string) string {
	fields := strings.Fields(spec)
	if len(fields) < 5 || !strings.Contains(fields[len(fields)-1], "7") {
		return spec
	}
	tz := ""
	if len(fields) == 6 && (strings.HasPrefix(fields[0], "CRON_TZ=") || strings.HasPrefix(fields[0], "TZ=")) {
		tz = fields[0]
		fields = fields[1:]
	}
	dow := len(fields) - 1 // the dow field (TZ prefixes were stripped above)
	var parts []string
	for _, tok := range strings.Split(fields[dow], ",") {
		step := ""
		if i := strings.Index(tok, "/"); i >= 0 {
			step, tok = tok[i:], tok[:i]
		}
		parts = append(parts, foldDowToken(tok, step)...)
	}
	fields[dow] = strings.Join(parts, ",")
	if tz != "" {
		fields = append([]string{tz}, fields...)
	}
	return strings.Join(fields, " ")
}

// foldDowToken rewrites one dow token (step already split off into step,
// "" when none), returning its folded form — possibly two tokens when a
// range ending at 7 wraps onto Sunday, and the step rides along so the
// schedule never runs more often than declared (a dropped step turned
// 1-7/2 into "every day").
func foldDowToken(tok, step string) []string {
	switch {
	case tok == "7":
		// Single value with a step does NOT expand (Vixie: 7/2 = {7},
		// not {0,2,4,6}) — fold to plain 0 either way.
		return []string{"0"}
	case tok == "*" && step != "":
		// Stepped star over 0-6 never exceeds 6 — */7 reads as {0};
		// pass through untouched.
		return []string{tok + step}
	case strings.HasSuffix(tok, "-7"):
		var lo int
		if n, _ := fmt.Sscanf(tok, "%d-7", &lo); n == 1 && lo >= 0 && lo <= 6 {
			out := []string{fmt.Sprintf("%d-6%s", lo, step)}
			// The step may land on Sunday (1-7/2 = {1,3,5,7} needs the
			// extra 0; 0-7/2 = {0,2,4,6} already keeps it in range).
			if step == "" || step == "/1" || landsOnSunday(lo, step) {
				out = append(out, "0")
			}
			return out
		}
	}
	return []string{tok}
}

func landsOnSunday(lo int, step string) bool {
	var s int
	if _, err := fmt.Sscanf(step, "/%d", &s); err != nil || s <= 0 {
		return true // no/invalid step = every value, Sunday included
	}
	return (7-lo)%s == 0
}
