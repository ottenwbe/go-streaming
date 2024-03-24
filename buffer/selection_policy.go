package buffer

import (
	"github.com/google/uuid"
	"go-stream-processing/events"
)

type PolicyID uuid.UUID

// SelectionPolicy defines how events are selected from a buffer
type SelectionPolicy interface {
	Apply(b Buffer) []events.Event
	ID() PolicyID
}

type SelectNextPolicy struct {
	id PolicyID
}

func NewSelectNPolicy(n int) SelectionPolicy {
	return &SelectNPolicy{
		N:  n,
		id: PolicyID(uuid.New()),
	}
}

func NewSelectNextPolicy() SelectionPolicy {
	return &SelectNextPolicy{
		id: PolicyID(uuid.New()),
	}
}

func (s *SelectNextPolicy) Apply(b Buffer) []events.Event {
	return []events.Event{b.GetNextEvent()}
}

func (s *SelectNextPolicy) ID() PolicyID {
	return s.id
}

type SelectNPolicy struct {
	N  int
	id PolicyID
}

func (s *SelectNPolicy) ID() PolicyID {
	return s.id
}

func (s *SelectNPolicy) Apply(buffer Buffer) []events.Event {
	eventBuffer := make([]events.Event, 0)

	for i := 0; i < s.N; i++ {
		eventBuffer = append(eventBuffer, buffer.GetAndRemoveNextEvent())
	}

	return eventBuffer
}
