package events

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/google/uuid"
)

// BufferReader allows read-only access to an underlying event buffer that implements BufferReader
type BufferReader[T any] interface {
	Get(i int) Event[T]
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

// PolicyID of each individual SelectionPolicy
type PolicyID uuid.UUID

const (
	CountingWindow = "counting"
	TemporalWindow = "temporal"
	SelectNext     = "selectNext"
)

// SelectionPolicyConfig is a serializable representation of a selection policy.
type SelectionPolicyConfig struct {
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
	// SelectionPolicy defines how events are selected from a buffer
	SelectionPolicy[T any] interface {
		NextSelectionReady() bool
		NextSelection() EventSelection
		UpdateSelection()
		Shift()
		Offset(offset int)
		ID() PolicyID
		SetBuffer(reader BufferReader[T])
		Description() SelectionPolicyConfig
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

func (s *CountingWindowPolicy[T]) Description() SelectionPolicyConfig {
	if s.n == 1 && s.shift == 1 {
		return SelectionPolicyConfig{Type: SelectNext}
	}
	return SelectionPolicyConfig{
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

func (s *TemporalWindowPolicy[T]) Description() SelectionPolicyConfig {
	return SelectionPolicyConfig{
		Type:         TemporalWindow,
		WindowStart:  s.windowStart,
		WindowLength: s.windowLength,
		WindowShift:  s.windowShift,
	}
}

// UpdateSelection updates the window based on the new event's timestamp
func (s *TemporalWindowPolicy[T]) UpdateSelection() {
	updateSelectionForBuffer(s.buffer, &s.currentRange, s.windowStart, s.windowEnd)
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
func NewCountingWindowPolicy[T any](n int, shift int) SelectionPolicy[T] {
	return &CountingWindowPolicy[T]{
		PolicyID:     PolicyID(uuid.New()),
		n:            n,
		shift:        shift,
		currentRange: EventSelection{0, -1},
	}
}

// NewSelectNextPolicy creates a new SelectNextPolicy
func NewSelectNextPolicy[T any]() SelectionPolicy[T] {
	return NewCountingWindowPolicy[T](1, 1)
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

func MakeSelectionPolicy(t string, size int, slide int, windowStart time.Time, windowLength time.Duration, windowShift time.Duration) SelectionPolicyConfig {
	return SelectionPolicyConfig{
		Active:       true,
		Type:         t,
		Size:         size,
		Slide:        slide,
		WindowStart:  windowStart,
		WindowLength: windowLength,
		WindowShift:  windowShift,
	}
}

// NewPolicyFromDescription creates a new SelectionPolicy from a SelectionPolicyConfig.
func NewPolicyFromDescription[T any](desc SelectionPolicyConfig) (SelectionPolicy[T], error) {
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

// PolicyDescriptionFromJSON parses a SelectionPolicyConfig from a JSON byte slice.
func PolicyDescriptionFromJSON(b []byte) (SelectionPolicyConfig, error) {
	var d SelectionPolicyConfig
	if err := json.Unmarshal(b, &d); err != nil {
		return SelectionPolicyConfig{}, err
	}
	return d, nil
}

// ToJSON converts a SelectionPolicyConfig to its JSON representation.
func (d SelectionPolicyConfig) ToJSON() ([]byte, error) {
	return json.Marshal(d)
}

// PolicyDescriptionFromYML parses a SelectionPolicyConfig from a YAML byte slice.
func PolicyDescriptionFromYML(b []byte) (SelectionPolicyConfig, error) {
	var d SelectionPolicyConfig
	if err := yaml.Unmarshal(b, &d); err != nil {
		return SelectionPolicyConfig{}, err
	}
	return d, nil
}

// ToYML converts a SelectionPolicyConfig to its YAML representation.
func (d SelectionPolicyConfig) ToYML() ([]byte, error) {
	return yaml.Marshal(d)
}

// MultiEventSelection represents a map of buffer indices to their selected ranges.
type MultiEventSelection map[int]EventSelection

// MultiSelectionPolicy defines how events are selected from multiple buffers
type MultiSelectionPolicy[T any] interface {
	NextSelectionReady() bool
	NextSelection() MultiEventSelection
	UpdateSelection()
	Shift()
	Offset(bufferIndex int, offset int)
	ID() PolicyID
	SetBuffers(readers map[int]BufferReader[T])
	Description() SelectionPolicyConfig
	AddCallback(callback func(map[int][]Event[T]))
}

// MultiTemporalWindowPolicy selects events based on a time window across multiple buffers.
type MultiTemporalWindowPolicy[T any] struct {
	PolicyID
	buffers      map[int]BufferReader[T]
	currentRange MultiEventSelection
	windowStart  time.Time
	windowEnd    time.Time
	windowLength time.Duration
	windowShift  time.Duration
	callbacks    []func(map[int][]Event[T])
	fired        bool
}

func (s *MultiTemporalWindowPolicy[T]) AddCallback(callback func(map[int][]Event[T])) {
	s.callbacks = append(s.callbacks, callback)
}

func (s *MultiTemporalWindowPolicy[T]) SetBuffers(buffers map[int]BufferReader[T]) {
	s.buffers = buffers
	s.currentRange = make(MultiEventSelection)
	for id := range buffers {
		s.currentRange[id] = EventSelection{0, -1}
	}
}

func (s *MultiTemporalWindowPolicy[T]) NextSelection() MultiEventSelection {
	return s.currentRange
}

func (s *MultiTemporalWindowPolicy[T]) NextSelectionReady() bool {
	if len(s.buffers) == 0 {
		return false
	}
	for _, buffer := range s.buffers {
		if buffer.Len() == 0 {
			return false
		}
		if !buffer.Get(buffer.Len() - 1).GetStamp().StartTime.After(s.windowEnd) {
			return false
		}
	}
	return true
}

func (s *MultiTemporalWindowPolicy[T]) UpdateSelection() {
	for id, buffer := range s.buffers {
		sel := s.currentRange[id]
		updateSelectionForBuffer(buffer, &sel, s.windowStart, s.windowEnd)
		s.currentRange[id] = sel
	}

	if s.NextSelectionReady() {
		s.triggerCallbacks()
	}
}

func (s *MultiTemporalWindowPolicy[T]) triggerCallbacks() {
	if s.fired || len(s.callbacks) == 0 {
		return
	}
	s.fired = true

	events := make(map[int][]Event[T])
	for id, sel := range s.currentRange {
		for i := sel.Start; i <= sel.End; i++ {
			events[id] = append(events[id], s.buffers[id].Get(i))
		}
	}
	for _, cb := range s.callbacks {
		cb(events)
	}
}

func (s *MultiTemporalWindowPolicy[T]) Shift() {
	s.windowStart = s.windowStart.Add(s.windowShift)
	s.windowEnd = s.windowStart.Add(s.windowLength)
	for id := range s.currentRange {
		sel := s.currentRange[id]
		sel.End = sel.Start - 1
		s.currentRange[id] = sel
	}
	s.fired = false
}

func (s *MultiTemporalWindowPolicy[T]) Offset(bufferIndex int, offset int) {
	if sel, ok := s.currentRange[bufferIndex]; ok {
		sel.Start = max(-1, sel.Start-offset)
		sel.End = max(-1, sel.End-offset)
		s.currentRange[bufferIndex] = sel
	}
}

func (s *MultiTemporalWindowPolicy[T]) Description() SelectionPolicyConfig {
	return SelectionPolicyConfig{
		Type:         TemporalWindow,
		WindowStart:  s.windowStart,
		WindowLength: s.windowLength,
		WindowShift:  s.windowShift,
	}
}

// NewMultiTemporalWindowPolicy creates a new MultiTemporalWindowPolicy
func NewMultiTemporalWindowPolicy[T any](startingTime time.Time, windowLength time.Duration, windowShift time.Duration) MultiSelectionPolicy[T] {
	return &MultiTemporalWindowPolicy[T]{
		PolicyID:     PolicyID(uuid.New()),
		currentRange: make(MultiEventSelection),
		windowStart:  startingTime,
		windowEnd:    startingTime.Add(windowLength),
		windowLength: windowLength,
		windowShift:  windowShift,
	}
}

// DuoPolicy defines how events are selected from two buffers of different types
type DuoPolicy[TLeft, TRight any] interface {
	NextSelectionReady() bool
	NextSelection() (EventSelection, EventSelection)
	UpdateSelection()
	Shift()
	Offset(leftOffset, rightOffset int)
	ID() PolicyID
	SetBuffers(left BufferReader[TLeft], right BufferReader[TRight])
	Description() SelectionPolicyConfig
	AddCallback(callback func([]Event[TLeft], []Event[TRight]))
}

// DuoTemporalWindowPolicy selects events based on a time window across two buffers.
type DuoTemporalWindowPolicy[TLeft, TRight any] struct {
	PolicyID
	bufferLeft   BufferReader[TLeft]
	bufferRight  BufferReader[TRight]
	rangeLeft    EventSelection
	rangeRight   EventSelection
	windowStart  time.Time
	windowEnd    time.Time
	windowLength time.Duration
	windowShift  time.Duration
	callbacks    []func([]Event[TLeft], []Event[TRight])
	fired        bool
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) SetBuffers(left BufferReader[TLeft], right BufferReader[TRight]) {
	s.bufferLeft = left
	s.bufferRight = right
	s.rangeLeft = EventSelection{0, -1}
	s.rangeRight = EventSelection{0, -1}
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) NextSelectionReady() bool {
	if s.bufferLeft == nil || s.bufferRight == nil {
		return false
	}
	if s.bufferLeft.Len() == 0 || s.bufferRight.Len() == 0 {
		return false
	}
	leftReady := s.bufferLeft.Get(s.bufferLeft.Len() - 1).GetStamp().StartTime.After(s.windowEnd)
	rightReady := s.bufferRight.Get(s.bufferRight.Len() - 1).GetStamp().StartTime.After(s.windowEnd)
	return leftReady && rightReady
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) NextSelection() (EventSelection, EventSelection) {
	return s.rangeLeft, s.rangeRight
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) UpdateSelection() {
	updateSelectionForBuffer(s.bufferLeft, &s.rangeLeft, s.windowStart, s.windowEnd)
	updateSelectionForBuffer(s.bufferRight, &s.rangeRight, s.windowStart, s.windowEnd)

	if s.NextSelectionReady() {
		s.triggerCallbacks()
	}
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) triggerCallbacks() {
	if s.fired || len(s.callbacks) == 0 {
		return
	}
	s.fired = true

	leftEvents := make([]Event[TLeft], 0)
	for i := s.rangeLeft.Start; i <= s.rangeLeft.End; i++ {
		leftEvents = append(leftEvents, s.bufferLeft.Get(i))
	}

	rightEvents := make([]Event[TRight], 0)
	for i := s.rangeRight.Start; i <= s.rangeRight.End; i++ {
		rightEvents = append(rightEvents, s.bufferRight.Get(i))
	}

	for _, cb := range s.callbacks {
		cb(leftEvents, rightEvents)
	}
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) Shift() {
	s.windowStart = s.windowStart.Add(s.windowShift)
	s.windowEnd = s.windowStart.Add(s.windowLength)
	s.rangeLeft.End = s.rangeLeft.Start - 1
	s.rangeRight.End = s.rangeRight.Start - 1
	s.fired = false
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) Offset(leftOffset, rightOffset int) {
	s.rangeLeft.Start = max(-1, s.rangeLeft.Start-leftOffset)
	s.rangeLeft.End = max(-1, s.rangeLeft.End-leftOffset)
	s.rangeRight.Start = max(-1, s.rangeRight.Start-rightOffset)
	s.rangeRight.End = max(-1, s.rangeRight.End-rightOffset)
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) Description() SelectionPolicyConfig {
	return SelectionPolicyConfig{
		Type:         TemporalWindow,
		WindowStart:  s.windowStart,
		WindowLength: s.windowLength,
		WindowShift:  s.windowShift,
	}
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) AddCallback(callback func([]Event[TLeft], []Event[TRight])) {
	s.callbacks = append(s.callbacks, callback)
}

func NewDuoTemporalWindowPolicy[TLeft, TRight any](startingTime time.Time, windowLength time.Duration, windowShift time.Duration) DuoPolicy[TLeft, TRight] {
	return &DuoTemporalWindowPolicy[TLeft, TRight]{
		PolicyID:     PolicyID(uuid.New()),
		windowStart:  startingTime,
		windowEnd:    startingTime.Add(windowLength),
		windowLength: windowLength,
		windowShift:  windowShift,
		rangeLeft:    EventSelection{0, -1},
		rangeRight:   EventSelection{0, -1},
	}
}

func updateSelectionForBuffer[T any](buffer BufferReader[T], currentRange *EventSelection, windowStart, windowEnd time.Time) {
	startScan := currentRange.End + 1
	for i := startScan; i < buffer.Len(); i++ {
		ts := buffer.Get(i).GetStamp().StartTime
		if ts.Before(windowStart) {
			currentRange.Start = i + 1
			currentRange.End = i
		} else if ts.After(windowEnd) || ts.Equal(windowEnd) {
			break
		} else {
			currentRange.End = i
		}
	}
}
