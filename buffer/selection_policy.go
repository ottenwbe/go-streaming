package buffer

import (
	"fmt"
	"github.com/google/uuid"
	"time"
)

// EventSelection represents a range of events within a buffer slice.
type EventSelection struct {
	Start int
	End   int
}

func (e EventSelection) IsValid() bool {
	return e.Start <= e.End && e.End > -1
}

type PolicyID uuid.UUID

// SelectionPolicy defines how events are selected from a buffer
type SelectionPolicy[T any] interface {
	NextSelectionReady() bool
	NextSelection() EventSelection
	UpdateSelection()
	Shift()
	Offset(offset int)
	ID() PolicyID
	SetBuffer(reader Reader[T])
}

type (
	SelectNextPolicy[T any] struct {
		PolicyID
		buffer         Reader[T]
		selectionReady bool
		next           int
	}
	CountingWindowPolicy[T any] struct {
		PolicyID
		buffer       Reader[T]
		n            int
		shift        int
		currentRange EventSelection
	}
	TemporalWindowPolicy[T any] struct {
		PolicyID
		buffer       Reader[T]
		currentRange EventSelection
		windowStart  time.Time
		windowEnd    time.Time
		windowLength time.Duration
		windowShift  time.Duration
	}
)

func (s *SelectNextPolicy[T]) NextSelection() EventSelection {
	return EventSelection{s.next, s.next}
}

func (s *SelectNextPolicy[T]) UpdateSelection() {
	potentialNext := s.next + 1
	if !s.selectionReady && s.buffer.Len() > potentialNext {
		s.next = potentialNext
		s.selectionReady = true
	}
}

func (s *SelectNextPolicy[T]) SetBuffer(buffer Reader[T]) {
	s.buffer = buffer
}

func (s *SelectNextPolicy[T]) Shift() {
	s.selectionReady = false
}

func (s *SelectNextPolicy[T]) Offset(offset int) {
	s.next -= offset
}

func (s *SelectNextPolicy[T]) NextSelectionReady() bool {
	return s.selectionReady
}

func (s *CountingWindowPolicy[T]) SetBuffer(buffer Reader[T]) {
	s.buffer = buffer
}

func (s *CountingWindowPolicy[T]) NextSelection() EventSelection {
	return s.currentRange
}

func (s *CountingWindowPolicy[T]) NextSelectionReady() bool {
	return s.currentRange.End-s.currentRange.Start+1 >= s.n
}

func (s *CountingWindowPolicy[T]) UpdateSelection() {

	fmt.Printf("Update buff p:%p l:%v %v\n", s.buffer, s.buffer.Len(), s.buffer)
	fmt.Printf("UpdateSelection #1 range: %v len: %v\n", s.currentRange, s.buffer.Len())
	for i := s.currentRange.End + 1; i < s.buffer.Len(); {
		fmt.Printf("UpdateSelection #2 %v %v\n", s.currentRange, s.buffer.Len())
		if s.currentRange.End-s.currentRange.Start+1 < s.n {
			s.currentRange.End++
			i++
		} else {
			i = s.buffer.Len()
		}
		fmt.Printf("UpdateSelection #3 %v %v %v\n", s.currentRange, s.buffer.Len(), i)
	}
}

func (s *CountingWindowPolicy[T]) Shift() {
	s.currentRange.Start = s.currentRange.Start + s.shift
	s.currentRange.End = s.currentRange.Start - 1
}

func (s *CountingWindowPolicy[T]) Offset(offset int) {
	s.currentRange.Start -= offset
	s.currentRange.End -= offset
}

// NextSelectionReady checks if there are no more events within the window
func (s *TemporalWindowPolicy[T]) NextSelectionReady() bool {
	return s.currentRange.End > -1 && s.buffer.Get(s.buffer.Len()-1).GetTimestamp().After(s.windowEnd)
}

// NextSelection returns the EventSelection for the current window
func (s *TemporalWindowPolicy[T]) NextSelection() EventSelection {
	return s.currentRange
}

// UpdateSelection updates the window based on the new event's timestamp
func (s *TemporalWindowPolicy[T]) UpdateSelection() {
	for i := s.currentRange.End + 1; i < s.buffer.Len(); {
		ts := s.buffer.Get(i).GetTimestamp()
		if ts.After(s.windowStart) && (ts.Before(s.windowEnd) || ts.Equal(s.windowEnd)) {
			s.currentRange.End = i
			if s.currentRange.Start < 0 {
				s.currentRange.Start = i
			}
		} else if ts.After(s.windowEnd) {
			i = s.buffer.Len()
		}
		i++
	}
}

// Shift is not relevant for time-based window and is left empty
func (s *TemporalWindowPolicy[T]) Shift() {
	s.windowStart = s.windowStart.Add(s.windowShift)
	s.windowEnd = s.windowStart.Add(s.windowLength)
	s.currentRange.End = s.currentRange.Start
}

func (s *TemporalWindowPolicy[T]) Offset(offset int) {
	s.currentRange.Start = max(-1, s.currentRange.Start-offset)
	s.currentRange.End = max(-1, s.currentRange.End-offset)
}

func (s *TemporalWindowPolicy[T]) SetBuffer(buffer Reader[T]) {
	s.buffer = buffer
}

func (id PolicyID) ID() PolicyID {
	return id
}

func (id PolicyID) String() string {
	return id.String()
}

func NewSelectNPolicy[T any](n int, shift int) SelectionPolicy[T] {
	return &CountingWindowPolicy[T]{
		PolicyID:     PolicyID(uuid.New()),
		n:            n,
		shift:        shift,
		currentRange: EventSelection{0, -1},
	}
}

func NewSelectNextPolicy[T any]() SelectionPolicy[T] {
	return &SelectNextPolicy[T]{
		PolicyID:       PolicyID(uuid.New()),
		selectionReady: false,
		next:           -1,
	}
}

// NewTemporalWindowPolicy creates a new TemporalWindowPolicy with the specified window and buffer
func NewTemporalWindowPolicy[T any](startingTime time.Time, windowLength time.Duration, windowShift time.Duration) SelectionPolicy[T] {

	return &TemporalWindowPolicy[T]{
		PolicyID:     PolicyID(uuid.New()),
		currentRange: EventSelection{0, -1},
		windowStart:  startingTime,
		windowEnd:    startingTime.Add(windowLength),
		windowLength: windowLength,
		windowShift:  windowShift,
	}
}
