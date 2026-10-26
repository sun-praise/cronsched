package cronexpr

import (
	"testing"
	"time"
)

// ref is a fixed reference instant: 2026-10-18 12:34:56 +08:00 (Saturday).
var ref = time.Date(2026, 10, 18, 12, 34, 56, 0,
	time.FixedZone("CST", 8*3600))

func mustNext(t *testing.T, spec string, after time.Time) time.Time {
	t.Helper()
	e, err := Parse(spec)
	if err != nil {
		t.Fatalf("Parse(%q): %v", spec, err)
	}
	return e.Next(after)
}

func TestNextBasicSchedules(t *testing.T) {
	cases := []struct {
		spec string
		want time.Time
	}{
		{"30 0 * * *", time.Date(2026, 10, 19, 0, 30, 0, 0, ref.Location())},       // daily 00:30
		{"34 12 * * *", time.Date(2026, 10, 19, 12, 34, 0, 0, ref.Location())},     // strictly after 12:34:56 → next day
		{"0 9 * * MON-FRI", time.Date(2026, 10, 19, 9, 0, 0, 0, ref.Location())},   // named weekdays; Sat→Mon
		{"*/15 9-17 * * *", time.Date(2026, 10, 18, 12, 45, 0, 0, ref.Location())}, // step within hours
		{"0 0 1 1 *", time.Date(2027, 1, 1, 0, 0, 0, 0, ref.Location())},           // yearly
	}
	for _, tc := range cases {
		if got := mustNext(t, tc.spec, ref); !got.Equal(tc.want) {
			t.Errorf("Next(%q) = %s, want %s", tc.spec, got, tc.want)
		}
	}
}

// TestNextDowZeroAndSevenBothSunday pins the 0/7 Sunday equivalence within
// the standard vocabulary.
func TestNextDowZeroAndSevenBothSunday(t *testing.T) {
	zero := mustNext(t, "0 22 * * 0", ref)
	seven := mustNext(t, "0 22 * * 7", ref)         // Vixie alias, folded by the normalizer
	steppedStar := mustNext(t, "0 22 * * */7", ref) // */7 reads as {0} — passthrough pinned
	rng := mustNext(t, "0 22 * * 1-7", ref)         // range ending at 7 folds to 1-6,0
	list := mustNext(t, "0 22 * * 5,7", ref)        // list member folds
	// Pins anchored from MONDAY 00:00 so a mutation cannot hide behind
	// "today is Sunday".
	mon := time.Date(2026, 10, 19, 0, 0, 0, 0, ref.Location())
	steppedStarMon := mustNext(t, "0 22 * * */7", mon)  // {0} → Sun 25th
	sevenSteppedMon := mustNext(t, "0 22 * * 7/2", mon) // single value never expands → Sun 25th
	if !zero.Equal(seven) {
		t.Fatalf("dow 0 and 7 diverge: %s vs %s", zero, seven)
	}
	if want := time.Date(2026, 10, 18, 22, 0, 0, 0, ref.Location()); !zero.Equal(want) {
		t.Fatalf("next sunday 22:00 = %s, want %s (today IS sunday)", zero, want)
	}
	if !rng.Equal(zero) || !list.Equal(zero) {
		t.Fatalf("range/list fold diverge: rng=%s list=%s zero=%s", rng, list, zero)
	}
	sun := time.Date(2026, 10, 25, 22, 0, 0, 0, ref.Location())
	todaySun := time.Date(2026, 10, 18, 22, 0, 0, 0, ref.Location())
	if !steppedStar.Equal(todaySun) {
		t.Fatalf("*/7 from Sunday ref = %s, want today 22:00 (passthrough keeps {0})", steppedStar)
	}
	if !steppedStarMon.Equal(sun) {
		t.Fatalf("*/7 from Monday = %s, want Sun 25th 22:00 (mutation: every-day star would say Mon 19th)", steppedStarMon)
	}
	if !sevenSteppedMon.Equal(sun) {
		t.Fatalf("7/2 from Monday = %s, want Sun 25th 22:00 (mutation: 0+step expansion would say Tue 20th)", sevenSteppedMon)
	}
}

// TestNextDomDowOrSemantics pins the Vixie OR rule: when both dom and dow
// are restricted, matching either day fires.
func TestNextDomDowOrSemantics(t *testing.T) {
	// The 13th (Tue) already passed before ref; sundays are the 18th/25th.
	// OR semantics: next fire is the earlier of (Nov 13, Oct 25 06:00) —
	// Oct 25 06:00 wins (Oct 18 06:00 is before the reference instant).
	got := mustNext(t, "0 6 13 * 0", ref)
	if want := time.Date(2026, 10, 25, 6, 0, 0, 0, ref.Location()); !got.Equal(want) {
		t.Fatalf("dom/dow OR = %s, want %s", got, want)
	}
}

func TestParseDescriptors(t *testing.T) {
	for _, spec := range []string{"@hourly", "@daily", "@weekly", "@monthly", "@yearly", "@every 2h"} {
		if _, err := Parse(spec); err != nil {
			t.Errorf("Parse(%q): %v", spec, err)
		}
	}
	if got := mustNext(t, "@daily", ref); got.Hour() != 0 || got.Day() != 19 {
		t.Errorf("@daily next = %s, want 2026-10-19 00:00", got)
	}
}

func TestParseTimezonePrefix(t *testing.T) {
	e, err := Parse("CRON_TZ=UTC 0 22 * * *")
	if err != nil {
		t.Fatal(err)
	}
	got := e.Next(ref) // 22:00 UTC = 06:00 +08 next day
	if want := time.Date(2026, 10, 19, 6, 0, 0, 0, ref.Location()); !got.Equal(want) {
		t.Fatalf("CRON_TZ next = %s, want %s", got, want)
	}
}

// TestNextImpossibleDateGivesUp pins the caller-side contract
// (Add rejects, the fire stamp disables): a never-firing spec yields the
// zero time. The give-up window is the parser's (5 years).
func TestNextImpossibleDateGivesUp(t *testing.T) {
	e, err := Parse("0 0 30 2 *") // Feb 30 never exists
	if err != nil {
		t.Fatal(err) // accepted at parse; the zero return is the contract
	}
	if !e.Next(ref).IsZero() {
		t.Fatalf("Next = %s, want zero time for an impossible date", e.Next(ref))
	}
}

// TestEverySubMinuteRejected pins the tight-loop guard: @every under one
// minute is refused at parse time.
func TestEverySubMinuteRejected(t *testing.T) {
	for _, spec := range []string{"@every 30s", "@every 1s", "@every 500ms"} {
		if _, err := Parse(spec); err == nil {
			t.Errorf("Parse(%q) succeeded, want a sub-minute @every rejection", spec)
		}
	}
	if _, err := Parse("@every 2m"); err != nil {
		t.Errorf("Parse(@every 2m): %v", err)
	}
}

func TestParseErrors(t *testing.T) {
	for _, spec := range []string{"", "60 * * * *", "* * * *", "* * * * * *", "not a spec", "0 25 * * *"} {
		if _, err := Parse(spec); err == nil {
			t.Errorf("Parse(%q) succeeded, want error", spec)
		}
	}
}

func TestStringRoundTrip(t *testing.T) {
	e, err := Parse("30 0 * * MON-FRI")
	if err != nil {
		t.Fatal(err)
	}
	if e.String() != "30 0 * * MON-FRI" {
		t.Fatalf("String = %q", e.String())
	}
}

// TestNextDOWStepRangeFold pins step ranges ending at 7: the fold must
// respect the step (1-7/2 = {1,3,5,7} → {1,3,5,0}).
func TestNextDOWStepRangeFold(t *testing.T) {
	// Anchors chosen to DISTINGUISH correct folding from a dropped step
	// (the first version's anchors passed with the broken "every day" set
	// too).
	//
	// 1-7/2 = {Mon,Wed,Fri,Sun}. From Tue 2026-10-20 00:00 the correct
	// next is Wed 21st 22:00; a dropped step (1-6 = Mon..Sat, daily-ish)
	// would answer TODAY (Tue 20th).
	tue := time.Date(2026, 10, 20, 0, 0, 0, 0, ref.Location())
	got := mustNext(t, "0 22 * * 1-7/2", tue)
	if want := time.Date(2026, 10, 21, 22, 0, 0, 0, ref.Location()); !got.Equal(want) {
		t.Fatalf("1-7/2 next = %s, want Wed 21st 22:00", got)
	}
	// 0-7/2 = {Sun,Tue,Thu,Sat} (the wrapped 0 dedups with 6). Anchored
	// AFTER Sunday 22:00 (Sun 25th 23:00) the correct next is TUE 27th
	// 22:00; a dropped step (0-6) would answer MON 26th — distinguishable.
	afterSun := time.Date(2026, 10, 25, 23, 0, 0, 0, ref.Location())
	got = mustNext(t, "0 22 * * 0-7/2", afterSun)
	if want := time.Date(2026, 10, 27, 22, 0, 0, 0, ref.Location()); !got.Equal(want) {
		t.Fatalf("0-7/2 next = %s, want Tue 27th 22:00", got)
	}
	// CRON_TZ + dow 7 combined (the fold used to no-op under a TZ prefix).
	// 02:00 UTC Sunday = 10:00 +08; the 18th's 02:00 UTC is already behind
	// the reference, so the next is the 25th.
	tzGot := mustNext(t, "CRON_TZ=UTC 0 2 * * 7", ref)
	if tzWant := time.Date(2026, 10, 25, 10, 0, 0, 0, ref.Location()); !tzGot.Equal(tzWant) {
		t.Fatalf("CRON_TZ dow-7 next = %s, want %s (02:00 UTC = 10:00 +08)", tzGot, tzWant)
	}
}
