package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/goccy/go-yaml"

	"github.com/google/uuid"
)

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
	CountingWindow      = "counting"
	TemporalWindow      = "temporal"
	SelectNext          = "selectNext"
	MultiTemporalWindow = "multiTemporal"
	DuoTemporalWindow   = "duoTemporal"
)

var (
	ErrUnknownPolicyType      = errors.New("unknown policy type")
	ErrUnknownMultiPolicyType = errors.New("unknown multi policy type")
	ErrUnknownDuoPolicyType   = errors.New("unknown duo policy type")
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

type SelectionOption func(*SelectionPolicyConfig)

// SelectNextOption allows to configure values for a select next window
func SelectNextOption() SelectionOption {
	return func(s *SelectionPolicyConfig) {
		s.Active = true
		s.Type = SelectNext
		s.Size = 1
		s.Slide = 1
	}
}

// CountingWindowOption allows to configure values for a counting window
func CountingWindowOption(size, slide int) SelectionOption {
	return func(s *SelectionPolicyConfig) {
		s.Active = true
		s.Type = CountingWindow
		s.Size = size
		s.Slide = slide
	}
}

// TemporalWindowOption allows to configure values for a temporal window
func TemporalWindowOption(windowStart time.Time, windowLength, windowShift time.Duration) SelectionOption {
	return func(s *SelectionPolicyConfig) {
		s.Active = true
		s.Type = TemporalWindow
		s.WindowStart = windowStart
		s.WindowLength = windowLength
		s.WindowShift = windowShift
	}
}

// MultiTemporalWindowOption allows to configure values for a multi-temporal window
func MultiTemporalWindowOption(windowStart time.Time, windowLength, windowShift time.Duration) SelectionOption {
	return func(s *SelectionPolicyConfig) {
		s.Active = true
		s.Type = MultiTemporalWindow
		s.WindowStart = windowStart
		s.WindowLength = windowLength
		s.WindowShift = windowShift
	}
}

// DuoTemporalWindowOption allows to configure values for a duo-temporal window
func DuoTemporalWindowOption(windowStart time.Time, windowLength, windowShift time.Duration) SelectionOption {
	return func(s *SelectionPolicyConfig) {
		s.Active = true
		s.Type = DuoTemporalWindow
		s.WindowStart = windowStart
		s.WindowLength = windowLength
		s.WindowShift = windowShift
	}
}

type (
	// SelectionPolicy defines how events are selected from a buffer
	SelectionPolicy[T any] interface {
		NextSelectionReady(buffer BufferReader[T]) bool
		NextSelection() EventSelection
		UpdateSelection(buffer BufferReader[T])
		Shift()
		Offset(offset int)
		ID() PolicyID
		Description() SelectionPolicyConfig
	}

	// CountingWindowPolicy selects a fixed number of events (n) with a sliding window (shift).
	CountingWindowPolicy[T any] struct {
		PolicyID
		n            int
		shift        int
		currentRange EventSelection
	}
	// TemporalWindowPolicy selects events based on a time window.
	TemporalWindowPolicy[T any] struct {
		PolicyID
		currentRange EventSelection
		windowStart  time.Time
		windowEnd    time.Time
		windowLength time.Duration
		windowShift  time.Duration
	}
)

func (s *CountingWindowPolicy[T]) NextSelection() EventSelection {
	return s.currentRange
}

func (s *CountingWindowPolicy[T]) NextSelectionReady(buffer BufferReader[T]) bool {
	return s.currentRange.End-s.currentRange.Start+1 >= s.n && buffer.Len() > s.currentRange.End
}

func (s *CountingWindowPolicy[T]) UpdateSelection(buffer BufferReader[T]) {
	available := buffer.Len() - s.currentRange.Start
	if available >= s.n {
		s.currentRange.End = s.currentRange.Start + s.n - 1
	} else {
		s.currentRange.End = buffer.Len() - 1
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
func (s *TemporalWindowPolicy[T]) NextSelectionReady(buffer BufferReader[T]) bool {
	if buffer.Len() == 0 {
		return false
	}
	return buffer.Get(buffer.Len() - 1).GetStamp().StartTime.After(s.windowEnd)
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
func (s *TemporalWindowPolicy[T]) UpdateSelection(buffer BufferReader[T]) {
	updateSelectionForBuffer(buffer, &s.currentRange, s.windowStart, s.windowEnd)
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

func MakeSelectionPolicy(option SelectionOption) SelectionPolicyConfig {
	config := SelectionPolicyConfig{}
	option(&config)
	return config
}

func MakeSelectionPolicyByValue(t string, size int, slide int, windowStart time.Time, windowLength time.Duration, windowShift time.Duration) SelectionPolicyConfig {
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

// NewSelectionPolicyFromConfig creates a new SelectionPolicy from a SelectionPolicyConfig.
func NewSelectionPolicyFromConfig[T any](desc SelectionPolicyConfig) (SelectionPolicy[T], error) {
	switch desc.Type {
	case CountingWindow:
		return NewCountingWindowPolicy[T](desc.Size, desc.Slide), nil
	case SelectNext:
		return NewSelectNextPolicy[T](), nil
	case TemporalWindow:
		return NewTemporalWindowPolicy[T](desc.WindowStart, desc.WindowLength, desc.WindowShift), nil
	default:
		return nil, fmt.Errorf("%w: '%s'", ErrUnknownPolicyType, desc.Type)
	}
}

func NewSelectionPolicy[T any](option SelectionOption) (SelectionPolicy[T], error) {
	config := SelectionPolicyConfig{}
	option(&config)
	return NewSelectionPolicyFromConfig[T](config)
}

// NewMultiSelectionPolicyFromConfig creates a new MultiSelectionPolicy from a SelectionPolicyConfig.
func NewMultiSelectionPolicyFromConfig[T any](desc SelectionPolicyConfig) (MultiSelectionPolicy[T], error) {
	switch desc.Type {
	case MultiTemporalWindow:
		return NewMultiTemporalWindowPolicy[T](desc.WindowStart, desc.WindowLength, desc.WindowShift), nil
	default:
		return nil, fmt.Errorf("%w: '%s'", ErrUnknownMultiPolicyType, desc.Type)
	}
}

func NewMultiSelectionPolicy[T any](option SelectionOption) (MultiSelectionPolicy[T], error) {
	config := MakeSelectionPolicy(option)
	return NewMultiSelectionPolicyFromConfig[T](config)
}

// NewDuoSelectionPolicyFromConfig creates a new DuoPolicy from a SelectionPolicyConfig.
func NewDuoSelectionPolicyFromConfig[TLeft, TRight any](desc SelectionPolicyConfig) (DuoPolicy[TLeft, TRight], error) {
	switch desc.Type {
	case DuoTemporalWindow:
		return NewDuoTemporalWindowPolicy[TLeft, TRight](desc.WindowStart, desc.WindowLength, desc.WindowShift), nil
	default:
		return nil, fmt.Errorf("%w: '%s'", ErrUnknownDuoPolicyType, desc.Type)
	}
}

func NewDuoSelectionPolicy[TLeft, TRight any](option SelectionOption) (DuoPolicy[TLeft, TRight], error) {
	config := SelectionPolicyConfig{}
	option(&config)
	return NewDuoSelectionPolicyFromConfig[TLeft, TRight](config)
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
	NextSelectionReady(buffers map[int]BufferReader[T]) bool
	NextSelection() MultiEventSelection
	UpdateSelection(buffers map[int]BufferReader[T])
	Shift()
	Offset(bufferIndex int, offset int)
	ID() PolicyID
	Description() SelectionPolicyConfig
	AddCallback(callback func(map[int][]Event[T]))
}

// MultiTemporalWindowPolicy selects events based on a time window across multiple buffers.
type MultiTemporalWindowPolicy[T any] struct {
	PolicyID
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

func (s *MultiTemporalWindowPolicy[T]) NextSelection() MultiEventSelection {
	return s.currentRange
}

func (s *MultiTemporalWindowPolicy[T]) NextSelectionReady(buffers map[int]BufferReader[T]) bool {
	if len(buffers) == 0 {
		return false
	}
	for _, buffer := range buffers {
		if buffer.Len() == 0 {
			return false
		}
		if !buffer.Get(buffer.Len() - 1).GetStamp().StartTime.After(s.windowEnd) {
			return false
		}
	}
	return true
}

func (s *MultiTemporalWindowPolicy[T]) UpdateSelection(buffers map[int]BufferReader[T]) {
	for id, buffer := range buffers {
		if _, ok := s.currentRange[id]; !ok {
			s.currentRange[id] = EventSelection{0, -1}
		}
		sel := s.currentRange[id]
		updateSelectionForBuffer(buffer, &sel, s.windowStart, s.windowEnd)
		s.currentRange[id] = sel
	}

	if s.NextSelectionReady(buffers) {
		s.triggerCallbacks(buffers)
	}
}

func (s *MultiTemporalWindowPolicy[T]) triggerCallbacks(buffers map[int]BufferReader[T]) {
	if s.fired || len(s.callbacks) == 0 {
		return
	}
	s.fired = true

	events := make(map[int][]Event[T])
	for id, sel := range s.currentRange {
		for i := sel.Start; i <= sel.End; i++ {
			events[id] = append(events[id], buffers[id].Get(i))
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
	NextSelectionReady(left BufferReader[TLeft], right BufferReader[TRight]) bool
	NextSelection() (EventSelection, EventSelection)
	UpdateSelection(left BufferReader[TLeft], right BufferReader[TRight])
	Shift()
	Offset(leftOffset, rightOffset int)
	ID() PolicyID
	Description() SelectionPolicyConfig
	AddCallback(callback func([]Event[TLeft], []Event[TRight]))
}

// DuoTemporalWindowPolicy selects events based on a time window across two buffers.
type DuoTemporalWindowPolicy[TLeft, TRight any] struct {
	PolicyID
	rangeLeft    EventSelection
	rangeRight   EventSelection
	windowStart  time.Time
	windowEnd    time.Time
	windowLength time.Duration
	windowShift  time.Duration
	callbacks    []func([]Event[TLeft], []Event[TRight])
	fired        bool
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) NextSelectionReady(left BufferReader[TLeft], right BufferReader[TRight]) bool {
	if left == nil || right == nil {
		return false
	}
	if left.Len() == 0 || right.Len() == 0 {
		return false
	}
	leftReady := left.Get(left.Len() - 1).GetStamp().StartTime.After(s.windowEnd)
	rightReady := right.Get(right.Len() - 1).GetStamp().StartTime.After(s.windowEnd)
	return leftReady && rightReady
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) NextSelection() (EventSelection, EventSelection) {
	return s.rangeLeft, s.rangeRight
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) UpdateSelection(left BufferReader[TLeft], right BufferReader[TRight]) {
	updateSelectionForBuffer(left, &s.rangeLeft, s.windowStart, s.windowEnd)
	updateSelectionForBuffer(right, &s.rangeRight, s.windowStart, s.windowEnd)

	if s.NextSelectionReady(left, right) {
		s.triggerCallbacks(left, right)
	}
}

func (s *DuoTemporalWindowPolicy[TLeft, TRight]) triggerCallbacks(left BufferReader[TLeft], right BufferReader[TRight]) {
	if s.fired || len(s.callbacks) == 0 {
		return
	}
	s.fired = true

	leftEvents := make([]Event[TLeft], 0)
	for i := s.rangeLeft.Start; i <= s.rangeLeft.End; i++ {
		leftEvents = append(leftEvents, left.Get(i))
	}

	rightEvents := make([]Event[TRight], 0)
	for i := s.rangeRight.Start; i <= s.rangeRight.End; i++ {
		rightEvents = append(rightEvents, right.Get(i))
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
