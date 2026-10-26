package cronsched

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemStore is an in-memory Store: schedules vanish with the process. It is
// the zero-dependency choice for tests and for schedulers whose state does
// not need to outlive a run.
type MemStore struct {
	mu       sync.Mutex
	nextID   int64
	entries  map[int64]*Schedule
	events   []Event
	eventSeq int64
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{nextID: 1, entries: make(map[int64]*Schedule)}
}

// Add upserts by name (see the Store contract).
func (m *MemStore) Add(ctx context.Context, s Schedule) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.entries {
		if e.Name == s.Name {
			e.CronExpr = s.CronExpr
			e.Action = s.Action
			e.Params = s.Params
			e.Enabled = s.Enabled
			e.NextFireAt = s.NextFireAt
			return e.ID, nil
		}
	}
	s.ID = m.nextID
	m.nextID++
	cp := s
	m.entries[s.ID] = &cp
	return s.ID, nil
}

// List returns all schedules sorted by name.
func (m *MemStore) List(ctx context.Context) ([]Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Schedule, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Remove drops one schedule by name.
func (m *MemStore) Remove(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, e := range m.entries {
		if e.Name == name {
			delete(m.entries, id)
			return nil
		}
	}
	return fmt.Errorf("%w: %q", ErrNotFound, name)
}

// SetEnabled toggles a schedule without losing its fire chain.
func (m *MemStore) SetEnabled(ctx context.Context, name string, enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.byNameLocked(name)
	if e == nil {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	e.Enabled = enabled
	return nil
}

// Due returns enabled, armed schedules whose NextFireAt has passed, oldest
// due first.
func (m *MemStore) Due(ctx context.Context, now float64) ([]Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Schedule
	for _, e := range m.entries {
		if e.Enabled && e.NextFireAt > 0 && e.NextFireAt <= now {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NextFireAt < out[j].NextFireAt })
	return out, nil
}

// RecordFire stamps the fire outcome and appends the audit row. A
// nextFireAt > 0 advances the chain; <= 0 disables the schedule (chain
// exhausted).
func (m *MemStore) RecordFire(ctx context.Context, id int64, result string, tookSeconds float64, nextFireAt float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[id]
	if e == nil {
		return fmt.Errorf("cronsched: record fire %d: no such schedule", id)
	}
	ts := unixSeconds(time.Now())
	e.LastFiredAt = ts
	e.LastResult = result
	if nextFireAt > 0 {
		e.NextFireAt = nextFireAt
	} else {
		e.Enabled = false
	}
	m.eventSeq++
	m.events = append(m.events, Event{
		ID: m.eventSeq, ScheduleID: id, Name: e.Name, FiredAt: ts,
		Action: e.Action, Result: result, DurationSeconds: tookSeconds,
	})
	return nil
}

// TriggerNow arms a schedule to fire on the next Due query.
func (m *MemStore) TriggerNow(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.byNameLocked(name)
	if e == nil || !e.Enabled {
		return fmt.Errorf("%w or disabled: %q", ErrNotFound, name)
	}
	e.NextFireAt = unixSeconds(time.Now())
	return nil
}

// RecentEvents returns the audit tail, newest first.
func (m *MemStore) RecentEvents(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 50
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Event, 0, len(m.events))
	for i := len(m.events) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, m.events[i])
	}
	return out, nil
}

// Earliest returns the soonest armed NextFireAt, if any.
func (m *MemStore) Earliest(ctx context.Context) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var earliest float64
	for _, e := range m.entries {
		if !e.Enabled || e.NextFireAt <= 0 {
			continue
		}
		if earliest == 0 || e.NextFireAt < earliest {
			earliest = e.NextFireAt
		}
	}
	if earliest <= 0 {
		return 0, false
	}
	return earliest, true
}

func (m *MemStore) byNameLocked(name string) *Schedule {
	for _, e := range m.entries {
		if e.Name == name {
			return e
		}
	}
	return nil
}
