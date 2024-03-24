package engine

import (
	"github.com/google/uuid"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
)

type PolicyID uuid.UUID

type SelectionPolicy interface {
	Apply(b buffer.Buffer) []events.Event
	ID() PolicyID
}

type SelectNextPolicy struct {
	id PolicyID
}

func (s *SelectNextPolicy) Apply(b buffer.Buffer) []events.Event {
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

func (s *SelectNPolicy) Apply(buffer buffer.Buffer) []events.Event {
	eventBuffer := make([]events.Event, 0)

	for i := 0; i < s.N; i++ {
		eventBuffer = append(eventBuffer, buffer.GetAndRemoveNextEvent())
	}

	return eventBuffer
}
