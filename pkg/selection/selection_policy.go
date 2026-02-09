package selection

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"gopkg.in/yaml.v3"

	"github.com/google/uuid"
)

// BufferReader allows read-only access to an underlying event buffer that implements BufferReader
type BufferReader[T any] interface {
	Get(i int) events.Event[T]
	Len() int
}

// EventSelection represents a range of events within a buffer slice.
type EventSelection struct {
	Start int
	End   int
}

// IsValid returns true if Start and End actually represent a possible selection in a buffer
func (e EventSelection) IsValid() bool {
	return e.Start <= e.End && e.End > -1 && e.Start > -1
}

// PolicyID of each individual Policy
type PolicyID uuid.UUID

const (
	CountingWindow = "counting"
	TemporalWindow = "temporal"
	SelectNext     = "selectNext"
)

// PolicyDescription is a serializable representation of a selection policy.
type PolicyDescription struct {
	Active bool   `json:"active" yaml:"active"`
	Type   string `json:"type" yaml:"type"`
	// For CountingWindowPolicy
	Size  int `json:"size,omitempty" yaml:"size,omitempty"`
	Slide int `json:"slide,omitempty" yaml:"slide,omitempty"`
	// For TemporalWindowPolicy
	WindowStart  time.Time     `json:"windowStart,omitempty" yaml:"windowStart,omitempty"`
	WindowLength time.Duration `json:"windowLength,omitempty" yaml:"windowLength,omitempty"`
	WindowShift  time.Duration `json:"windowShift,omitempty" yaml:"windowShift,omitempty"`
}

type (
	// Policy defines how events are selected from a buffer
	Policy[T any] interface {
		NextSelectionReady() bool
		NextSelection() EventSelection
		UpdateSelection()
		Shift()
		Offset(offset int)
		ID() PolicyID
		SetBuffer(reader BufferReader[T])
		Description() PolicyDescription
	}

	// CountingWindowPolicy selects a fixed number of events (n) with a sliding window (shift).
	CountingWindowPolicy[T any] struct {
		PolicyID
		buffer       BufferReader[T]
		n            int
		shift        int
		currentRange EventSelection
	}
	// TemporalWindowPolicy selects events based on a time window.
	TemporalWindowPolicy[T any] struct {
		PolicyID
		buffer       BufferReader[T]
		currentRange EventSelection
		windowStart  time.Time
		windowEnd    time.Time
		windowLength time.Duration
		windowShift  time.Duration
	}
)

func (s *CountingWindowPolicy[T]) SetBuffer(buffer BufferReader[T]) {
	s.buffer = buffer
}

func (s *CountingWindowPolicy[T]) NextSelection() EventSelection {
	return s.currentRange
}

func (s *CountingWindowPolicy[T]) NextSelectionReady() bool {
	return s.currentRange.End-s.currentRange.Start+1 >= s.n
}

func (s *CountingWindowPolicy[T]) UpdateSelection() {
	available := s.buffer.Len() - s.currentRange.Start
	if available >= s.n {
		s.currentRange.End = s.currentRange.Start + s.n - 1
	} else {
		s.currentRange.End = s.buffer.Len() - 1
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

func (s *CountingWindowPolicy[T]) Description() PolicyDescription {
	if s.n == 1 && s.shift == 1 {
		return PolicyDescription{Type: SelectNext}
	}
	return PolicyDescription{
		Type:  CountingWindow,
		Size:  s.n,
		Slide: s.shift,
	}
}

// NextSelectionReady checks if there are no more events within the window
func (s *TemporalWindowPolicy[T]) NextSelectionReady() bool {
	if s.buffer.Len() == 0 {
		return false
	}
	return s.buffer.Get(s.buffer.Len() - 1).GetStamp().StartTime.After(s.windowEnd)
}

// NextSelection returns the EventSelection for the current window
func (s *TemporalWindowPolicy[T]) NextSelection() EventSelection {
	return s.currentRange
}

func (s *TemporalWindowPolicy[T]) Description() PolicyDescription {
	return PolicyDescription{
		Type:         TemporalWindow,
		WindowStart:  s.windowStart,
		WindowLength: s.windowLength,
		WindowShift:  s.windowShift,
	}
}

// UpdateSelection updates the window based on the new event's timestamp
func (s *TemporalWindowPolicy[T]) UpdateSelection() {
	startScan := s.currentRange.End + 1
	for i := startScan; i < s.buffer.Len(); i++ {
		ts := s.buffer.Get(i).GetStamp().StartTime

		if ts.Before(s.windowStart) {
			s.currentRange.Start = i + 1
			s.currentRange.End = i
		} else if ts.After(s.windowEnd) || ts.Equal(s.windowEnd) {
			break
		} else {
			s.currentRange.End = i
		}
	}
}

// Shift is not relevant for time-based window and is left empty
func (s *TemporalWindowPolicy[T]) Shift() {
	s.windowStart = s.windowStart.Add(s.windowShift)
	s.windowEnd = s.windowStart.Add(s.windowLength)
	s.currentRange.End = s.currentRange.Start - 1
}

func (s *TemporalWindowPolicy[T]) Offset(offset int) {
	s.currentRange.Start = max(-1, s.currentRange.Start-offset)
	s.currentRange.End = max(-1, s.currentRange.End-offset)
}

func (s *TemporalWindowPolicy[T]) SetBuffer(buffer BufferReader[T]) {
	s.buffer = buffer
}

func (id PolicyID) ID() PolicyID {
	return id
}

func (id PolicyID) String() string {
	return id.String()
}

// NewCountingWindowPolicy creates a new CountingWindowPolicy
func NewCountingWindowPolicy[T any](n int, shift int) Policy[T] {
	return &CountingWindowPolicy[T]{
		PolicyID:     PolicyID(uuid.New()),
		n:            n,
		shift:        shift,
		currentRange: EventSelection{0, -1},
	}
}

// NewSelectNextPolicy creates a new SelectNextPolicy
func NewSelectNextPolicy[T any]() Policy[T] {
	return NewCountingWindowPolicy[T](1, 1)
}

// NewTemporalWindowPolicy creates a new TemporalWindowPolicy with the specified window and buffer
func NewTemporalWindowPolicy[T any](startingTime time.Time, windowLength time.Duration, windowShift time.Duration) Policy[T] {

	return &TemporalWindowPolicy[T]{
		PolicyID:     PolicyID(uuid.New()),
		currentRange: EventSelection{0, -1},
		windowStart:  startingTime,
		windowEnd:    startingTime.Add(windowLength),
		windowLength: windowLength,
		windowShift:  windowShift,
	}
}

func MakePolicy(t string, size int, slide int, windowStart time.Time, windowLength time.Duration, windowShift time.Duration) PolicyDescription {
	return PolicyDescription{
		Active:       true,
		Type:         t,
		Size:         size,
		Slide:        slide,
		WindowStart:  windowStart,
		WindowLength: windowLength,
		WindowShift:  windowShift,
	}
}

// NewPolicyFromDescription creates a new Policy from a PolicyDescription.
func NewPolicyFromDescription[T any](desc PolicyDescription) (Policy[T], error) {
	switch desc.Type {
	case CountingWindow:
		return NewCountingWindowPolicy[T](desc.Size, desc.Slide), nil
	case SelectNext:
		return NewSelectNextPolicy[T](), nil
	case TemporalWindow:
		return NewTemporalWindowPolicy[T](desc.WindowStart, desc.WindowLength, desc.WindowShift), nil
	default:
		return nil, fmt.Errorf("unknown policy type: '%s'", desc.Type)
	}
}

// PolicyDescriptionFromJSON parses a PolicyDescription from a JSON byte slice.
func PolicyDescriptionFromJSON(b []byte) (PolicyDescription, error) {
	var d PolicyDescription
	if err := json.Unmarshal(b, &d); err != nil {
		return PolicyDescription{}, err
	}
	return d, nil
}

// ToJSON converts a PolicyDescription to its JSON representation.
func (d PolicyDescription) ToJSON() ([]byte, error) {
	return json.Marshal(d)
}

// PolicyDescriptionFromYML parses a PolicyDescription from a YAML byte slice.
func PolicyDescriptionFromYML(b []byte) (PolicyDescription, error) {
	var d PolicyDescription
	if err := yaml.Unmarshal(b, &d); err != nil {
		return PolicyDescription{}, err
	}
	return d, nil
}

// ToYML converts a PolicyDescription to its YAML representation.
func (d PolicyDescription) ToYML() ([]byte, error) {
	return yaml.Marshal(d)
}
