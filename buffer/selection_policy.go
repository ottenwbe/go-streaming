package buffer

import (
	"github.com/google/uuid"
	"go-stream-processing/events"
)

// EventRange represents a range of events within the buffer slice.
type EventRange struct {
	Start int
	End   int
}

type PolicyID uuid.UUID

// SelectionPolicy defines how events are selected from a buffer
type SelectionPolicy[T any] interface {
	NextSelectionReady() bool
	NextSelection() EventRange
	UpdateSelection(event events.Event[T])
	ShiftWithOffset(offset int)
	ID() PolicyID
}

type SelectNextPolicy[T any] struct {
	id             PolicyID
	selectionReady bool
	next           int
}

func (s *SelectNextPolicy[T]) NextSelection() EventRange {
	return EventRange{s.next, s.next}
}

func (s *SelectNextPolicy[T]) UpdateSelection(event events.Event[T]) {
	if !s.selectionReady {
		s.next++
		s.selectionReady = true
	}
}

func (s *SelectNextPolicy[T]) ShiftWithOffset(offset int) {
	s.selectionReady = false
	s.next -= offset + 1
}

func (s *SelectNextPolicy[T]) NextSelectionReady() bool {
	return s.selectionReady
}

func (s *SelectNextPolicy[T]) ID() PolicyID {
	return s.id
}

type SelectNPolicy[T any] struct {
	n            int
	currentRange EventRange
	id           PolicyID
}

func (s *SelectNPolicy[T]) NextSelection() EventRange {
	return s.currentRange
}

func (s *SelectNPolicy[T]) NextSelectionReady() bool {
	return s.currentRange.End-s.currentRange.Start+1 >= s.n
}

func (s *SelectNPolicy[T]) UpdateSelection(event events.Event[T]) {
	if s.currentRange.End-s.currentRange.Start+1 < s.n {
		s.currentRange.End++
	}
}

func (s *SelectNPolicy[T]) ShiftWithOffset(offset int) {
	s.currentRange.Start = s.currentRange.End - offset
	s.currentRange.End -= offset + 1
}

func (s *SelectNPolicy[T]) ID() PolicyID {
	return s.id
}

func NewSelectNPolicy[T any](n int) SelectionPolicy[T] {
	return &SelectNPolicy[T]{
		n:            n,
		currentRange: EventRange{0, -1},
		id:           PolicyID(uuid.New()),
	}
}

func NewSelectNextPolicy[T any]() SelectionPolicy[T] {
	return &SelectNextPolicy[T]{
		id:             PolicyID(uuid.New()),
		selectionReady: false,
		next:           -1,
	}
}
