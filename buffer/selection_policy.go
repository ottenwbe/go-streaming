package buffer

import (
	"github.com/google/uuid"
	"go-stream-processing/events"
)

type SelectionPolicy interface {
	Apply(b Buffer) []events.Event
	ID() uuid.UUID
}

type SelectNextPolicy struct {
	id uuid.UUID
}

func (s *SelectNextPolicy) Apply(b Buffer) []events.Event {
	return []events.Event{b.GetNextEvent()}
}

func (s *SelectNextPolicy) ID() uuid.UUID {
	return s.id
}

type SelectNPolicy struct {
	N  int
	id uuid.UUID
}

func (s *SelectNPolicy) ID() uuid.UUID {
	return s.id
}

func (s *SelectNPolicy) Apply(buffer Buffer) []events.Event {
	eventBuffer := make([]events.Event, 0)

	for i := 0; i < s.N; i++ {
		eventBuffer = append(eventBuffer, buffer.GetAndRemoveNextEvent())
	}

	return eventBuffer
}
