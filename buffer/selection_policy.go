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
type SelectionPolicy interface {
	NextSelectionReady() bool
	NextSelection() EventRange
	UpdateSelection(event events.Event)
	ShiftWithOffset(offset int)
	ID() PolicyID
}

type SelectNextPolicy struct {
	id             PolicyID
	selectionReady bool
	next           int
}

func (s *SelectNextPolicy) NextSelection() EventRange {
	return EventRange{s.next, s.next}
}

func (s *SelectNextPolicy) UpdateSelection(event events.Event) {
	if !s.selectionReady {
		s.next++
		s.selectionReady = true
	}
}

func (s *SelectNextPolicy) ShiftWithOffset(offset int) {
	s.selectionReady = false
	s.next -= offset
}

func (s *SelectNextPolicy) NextSelectionReady() bool {
	return s.selectionReady
}

func (s *SelectNextPolicy) ID() PolicyID {
	return s.id
}

type SelectNPolicy struct {
	n            int
	currentRange EventRange
	id           PolicyID
}

func (s *SelectNPolicy) NextSelection() EventRange {
	return s.currentRange
}

func (s *SelectNPolicy) NextSelectionReady() bool {
	return s.currentRange.End-s.currentRange.Start+1 >= s.n
}

func (s *SelectNPolicy) UpdateSelection(event events.Event) {
	if s.currentRange.End-s.currentRange.Start+1 < s.n {
		s.currentRange.End++
	}
}

func (s *SelectNPolicy) ShiftWithOffset(offset int) {
	s.currentRange.Start = s.currentRange.End - offset
	s.currentRange.End -= offset

}

func (s *SelectNPolicy) ID() PolicyID {
	return s.id
}

func NewSelectNPolicy(n int) SelectionPolicy {
	return &SelectNPolicy{
		n:            n,
		currentRange: EventRange{0, 0},
		id:           PolicyID(uuid.New()),
	}
}

func NewSelectNextPolicy() SelectionPolicy {
	return &SelectNextPolicy{
		id:             PolicyID(uuid.New()),
		selectionReady: false,
		next:           -1,
	}
}
