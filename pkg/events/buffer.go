package events

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
)

var defaultBufferCapacity = 5

var (
	ErrBufferStopped = errors.New("buffer: is stopped")
	ErrLimitExceeded = errors.New("buffer: limit exceeded")
)

// EventBuffer is a simple, non-concurrent, in-memory buffer for events.
// It is not safe for concurrent use without external locking. It implements BufferReader.
type EventBuffer[T any] struct {
	events []Event[T]
}

// NewEventBuffer creates a new, empty EventBuffer.
func NewEventBuffer[T any]() *EventBuffer[T] {
	return &EventBuffer[T]{
		events: make([]Event[T], 0),
	}
}

// Get returns the event at the given index.
func (b *EventBuffer[T]) Get(i int) Event[T] {
	return b.events[i]
}

// Len returns the number of events in the buffer.
func (b *EventBuffer[T]) Len() int {
	return len(b.events)
}

// Add appends an event to the buffer.
func (b *EventBuffer[T]) Add(e Event[T]) {
	b.events = append(b.events, e)
}

// Remove removes the first n events from the buffer.
func (b *EventBuffer[T]) Remove(n int) {
	if n <= 0 {
		return
	}
	if n > len(b.events) {
		n = len(b.events)
	}
	for i := 0; i < n; i++ {
		b.events[i] = nil
	}
	b.events = b.events[n:]
}

// basicBuffer is an (internal) alias for an event array
type basicBuffer[T any] []Event[T]

// Buffer interface to interact with any buffer
type Buffer[T any] interface {
	GetAndConsumeNextEvents() []Event[T]
	PeekNextEvent() Event[T]
	AddEvent(event Event[T]) error
	AddEvents(events []Event[T]) error
	Len() int
	Get(x int) Event[T]
	Dump() []Event[T]
	StopBlocking()
	StartBlocking()
	GetAndRemoveNextEvent() Event[T]
}

type Iterator[T any] struct {
	x      int
	buffer Buffer[T]
}

// asyncBuffer represents an asynchronous buffer.
// This is the base for all other buffers, like the SimpleAsyncBuffer
type asyncBuffer[T any] struct {
	buffer      basicBuffer[T]
	id          uuid.UUID
	bufferMutex sync.Mutex
	cond        *sync.Cond
	stopped     bool
}

// SimpleAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations GetNextEvent and RemoveNextEvent either return the Next event,
// if any is available in the buffer or wait until Next event is available.
type SimpleAsyncBuffer[T any] struct {
	*asyncBuffer[T]
}

// SortedSimpleAsyncBuffer is a buffer that ensures events are strictly ordered by their StartTime.
type SortedSimpleAsyncBuffer[T any] struct {
	*SimpleAsyncBuffer[T]
	limit int
}

// LimitedSimpleAsyncBuffer is a buffer with a fixed capacity limit.
type LimitedSimpleAsyncBuffer[T any] struct {
	*SimpleAsyncBuffer[T]
	limit int
}

// LimitedConsumableAsyncBuffer is a buffer with a fixed capacity limit that consumes events based on a policy.
type LimitedConsumableAsyncBuffer[T any] struct {
	*ConsumableAsyncBuffer[T]
	limit int
}

// ConsumableAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations PeekNextEvent and RemoveNextEvent either return the Next event,
// if any is available in the buffer or wait until Next event is available based on a selection policy.
// see selection.Policy[T]
type ConsumableAsyncBuffer[T any] struct {
	*asyncBuffer[T]
	selectionPolicy Policy[T]
}

func (b basicBuffer[T]) Get(i int) Event[T] {
	return b[i]
}

func (b basicBuffer[T]) Len() int {
	return len(b)
}

func newAsyncBuffer[T any]() *asyncBuffer[T] {
	s := &asyncBuffer[T]{
		buffer:      make(basicBuffer[T], 0, defaultBufferCapacity),
		bufferMutex: sync.Mutex{},
		stopped:     false,
		id:          uuid.New(),
	}
	s.cond = sync.NewCond(&s.bufferMutex)
	return s
}

// NewSimpleAsyncBuffer creates a new unbounded asynchronous buffer.
func NewSimpleAsyncBuffer[T any]() Buffer[T] {
	s := &SimpleAsyncBuffer[T]{
		asyncBuffer: newAsyncBuffer[T](),
	}
	return s
}

// NewSortedSimpleAsyncBuffer creates a new asynchronous buffer that sorts events by timestamp on insertion.
// A limit <= 0 means the buffer is unbounded.
func NewSortedSimpleAsyncBuffer[T any](limit int) Buffer[T] {
	s := &SortedSimpleAsyncBuffer[T]{
		SimpleAsyncBuffer: &SimpleAsyncBuffer[T]{
			asyncBuffer: newAsyncBuffer[T](),
		},
		limit: limit,
	}
	return s
}

// NewLimitedSimpleAsyncBuffer creates a new asynchronous buffer with a maximum event limit.
func NewLimitedSimpleAsyncBuffer[T any](limit int) Buffer[T] {
	s := &LimitedSimpleAsyncBuffer[T]{
		SimpleAsyncBuffer: &SimpleAsyncBuffer[T]{
			asyncBuffer: newAsyncBuffer[T](),
		},
		limit: limit,
	}
	return s
}

// NewLimitedConsumableAsyncBuffer creates a new buffer that consumes events based on a selection policy with a maximum event limit.
func NewLimitedConsumableAsyncBuffer[T any](policy Policy[T], limit int) Buffer[T] {
	s := &LimitedConsumableAsyncBuffer[T]{
		ConsumableAsyncBuffer: &ConsumableAsyncBuffer[T]{
			asyncBuffer:     newAsyncBuffer[T](),
			selectionPolicy: policy,
		},
		limit: limit,
	}
	s.selectionPolicy.SetBuffer(&s.buffer)
	return s
}

// NewConsumableAsyncBuffer creates a new buffer that consumes events based on a selection policy.
func NewConsumableAsyncBuffer[T any](policy Policy[T]) Buffer[T] {
	s := &ConsumableAsyncBuffer[T]{
		asyncBuffer:     newAsyncBuffer[T](),
		selectionPolicy: policy,
	}
	s.selectionPolicy.SetBuffer(&s.buffer)
	return s
}

func (s *asyncBuffer[T]) StartBlocking() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.stopped = false
}

func (s *asyncBuffer[T]) StopBlocking() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.stopped = true
	s.cond.Broadcast()
}

func (s *asyncBuffer[T]) Len() int {
	return len(s.buffer)
}

func (s *asyncBuffer[T]) Get(i int) Event[T] {
	return s.buffer[i]
}

func (s *asyncBuffer[T]) Dump() []Event[T] {
	destination := make([]Event[T], s.Len())
	copy(destination, s.buffer)
	return destination
}

// PeekNextEvent returns the Next buffered event, but no event will be removed from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *asyncBuffer[T]) PeekNextEvent() Event[T] {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.Len() == 0 && !s.stopped {
		s.cond.Wait()
	}

	if s.Len() > 0 {
		return s.buffer[0]
	}
	return nil
}

func (s *asyncBuffer[T]) GetAndRemoveNextEvent() Event[T] {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	var e Event[T] = nil

	if s.Len() == 0 && !s.stopped {
		s.cond.Wait()
	}

	if s.Len() > 0 {
		e = s.buffer[0]
		s.buffer[0] = nil
		s.buffer = s.buffer[1:]
		return e
	}

	return nil
}

// GetAndConsumeNextEvents returns the Next event from the buffer and removes it.
func (s *SimpleAsyncBuffer[T]) GetAndConsumeNextEvents() []Event[T] {
	e := s.asyncBuffer.GetAndRemoveNextEvent()
	if e != nil {
		return []Event[T]{e}
	}
	return nil
}

// AddEvents adds multiple events to the buffer.
func (s *SimpleAsyncBuffer[T]) AddEvents(events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	s.buffer = append(s.buffer, events...)
	s.cond.Broadcast()
	return nil
}

// AddEvent adds a single event to the buffer.
func (s *SimpleAsyncBuffer[T]) AddEvent(event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	s.buffer = append(s.buffer, event)
	s.cond.Broadcast()
	return nil
}

// AddEvents adds multiple events to the buffer and sorts them by StartTime.
func (s *SortedSimpleAsyncBuffer[T]) AddEvents(events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	if s.limit > 0 {
		if len(events) > s.limit {
			return fmt.Errorf("%w: %d > %d", ErrLimitExceeded, len(events), s.limit)
		}
		for s.buffer.Len()+len(events) > s.limit {
			s.cond.Wait()
			if s.stopped {
				return ErrBufferStopped
			}
		}
	}

	s.buffer = append(s.buffer, events...)

	// Sort the buffer based on StartTime
	sort.SliceStable(s.buffer, func(i, j int) bool {
		return s.buffer[i].GetStamp().StartTime.Before(s.buffer[j].GetStamp().StartTime)
	})

	s.cond.Broadcast()
	return nil
}

// AddEvent adds a single event to the buffer and sorts it into position.
func (s *SortedSimpleAsyncBuffer[T]) AddEvent(event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	if s.limit > 0 {
		for s.buffer.Len() >= s.limit {
			s.cond.Wait()
			if s.stopped {
				return ErrBufferStopped
			}
		}
	}

	s.buffer = append(s.buffer, event)
	sort.SliceStable(s.buffer, func(i, j int) bool {
		return s.buffer[i].GetStamp().StartTime.Before(s.buffer[j].GetStamp().StartTime)
	})

	s.cond.Broadcast()
	return nil
}

// GetAndConsumeNextEvents returns the Next event from the buffer and removes it.
func (s *SortedSimpleAsyncBuffer[T]) GetAndConsumeNextEvents() []Event[T] {
	nextEvents := s.SimpleAsyncBuffer.GetAndConsumeNextEvents()
	if s.limit > 0 && len(nextEvents) > 0 {
		s.cond.Broadcast()
	}
	return nextEvents
}

// GetAndRemoveNextEvent returns the Next event from the buffer and removes it.
func (s *SortedSimpleAsyncBuffer[T]) GetAndRemoveNextEvent() Event[T] {
	event := s.SimpleAsyncBuffer.GetAndRemoveNextEvent()
	if s.limit > 0 && event != nil {
		s.cond.Broadcast()
	}
	return event
}

// GetAndConsumeNextEvents returns the Next buffered events and removes this event from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *ConsumableAsyncBuffer[T]) GetAndConsumeNextEvents() []Event[T] {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	var (
		selectedEvents basicBuffer[T]
		selectionFound = s.selectionPolicy.NextSelectionReady()
		selection      EventSelection
	)

	// Wait until the buffer has enough events
	for !selectionFound && !s.stopped {
		s.cond.Wait()
		selectionFound = s.selectionPolicy.NextSelectionReady()
	}

	selection = s.selectionPolicy.NextSelection()
	if selection.IsValid() { // Selection is in some cases not valid if s.stopped
		// Extract selected events from the buffer
		src := s.buffer[selection.Start : selection.End+1]
		selectedEvents = make([]Event[T], len(src))
		copy(selectedEvents, src)
	}

	if selectionFound {
		s.selectionPolicy.Shift()
		s.selectionPolicy.UpdateSelection()
		offset := s.selectionPolicy.NextSelection().Start

		removeCount := offset
		if removeCount > len(s.buffer) {
			removeCount = len(s.buffer)
		}

		for i := 0; i < removeCount; i++ {
			s.buffer[i] = nil
		}
		s.buffer = s.buffer[removeCount:]
		s.selectionPolicy.Offset(removeCount)

		return selectedEvents
	}
	return nil
}

// AddEvents adds multiple events to the buffer and updates the selection policy.
func (s *ConsumableAsyncBuffer[T]) AddEvents(events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	s.buffer = append(s.buffer, events...)
	s.selectionPolicy.UpdateSelection()

	s.cond.Broadcast()
	return nil
}

// AddEvent adds a single event to the buffer and updates the selection policy.
func (s *ConsumableAsyncBuffer[T]) AddEvent(event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	s.buffer = append(s.buffer, event)
	s.selectionPolicy.UpdateSelection()

	s.cond.Broadcast()
	return nil
}

// AddEvents adds multiple events to the buffer, ensuring the limit is not exceeded.
func (s *LimitedSimpleAsyncBuffer[T]) AddEvents(events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if len(events) > s.limit {
		return fmt.Errorf("%w: %d > %d", ErrLimitExceeded, len(events), s.limit)
	}
	if s.stopped {
		return ErrBufferStopped
	}

	for s.buffer.Len()+len(events) > s.limit {
		s.cond.Wait()
	}

	s.buffer = append(s.buffer, events...)
	s.cond.Broadcast()
	return nil
}

// AddEvent adds a single event to the buffer, ensuring the limit is not exceeded.
func (s *LimitedSimpleAsyncBuffer[T]) AddEvent(event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	for s.buffer.Len() >= s.limit {
		s.cond.Wait()
	}

	s.buffer = append(s.buffer, event)
	s.cond.Broadcast()
	return nil
}

// GetAndConsumeNextEvents returns the Next event from the buffer and removes it.
func (s *LimitedSimpleAsyncBuffer[T]) GetAndConsumeNextEvents() []Event[T] {
	nextEvents := s.SimpleAsyncBuffer.GetAndConsumeNextEvents()

	if len(nextEvents) > 0 {
		s.cond.Broadcast()
	}
	return nextEvents
}

// GetAndRemoveNextEvent returns the Next event from the buffer and removes it.
func (s *LimitedSimpleAsyncBuffer[T]) GetAndRemoveNextEvent() Event[T] {
	event := s.SimpleAsyncBuffer.GetAndRemoveNextEvent()
	if event != nil {
		s.cond.Broadcast()
	}
	return event
}

// AddEvents adds multiple events to the buffer, ensuring the limit is not exceeded.
func (s *LimitedConsumableAsyncBuffer[T]) AddEvents(events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if len(events) > s.limit {
		return fmt.Errorf("%w: %d > %d", ErrLimitExceeded, len(events), s.limit)
	}
	if s.stopped {
		return ErrBufferStopped
	}

	for s.buffer.Len()+len(events) > s.limit {
		s.cond.Wait()
	}

	s.buffer = append(s.buffer, events...)
	s.selectionPolicy.UpdateSelection()

	s.cond.Broadcast()
	return nil
}

// AddEvent adds a single event to the buffer, ensuring the limit is not exceeded.
func (s *LimitedConsumableAsyncBuffer[T]) AddEvent(event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	for s.buffer.Len() >= s.limit {
		s.cond.Wait()
	}

	s.buffer = append(s.buffer, event)
	s.selectionPolicy.UpdateSelection()

	s.cond.Broadcast()
	return nil
}

// GetAndConsumeNextEvents returns the Next event from the buffer and removes it.
func (s *LimitedConsumableAsyncBuffer[T]) GetAndConsumeNextEvents() []Event[T] {
	nextEvents := s.ConsumableAsyncBuffer.GetAndConsumeNextEvents()

	if len(nextEvents) > 0 {
		s.cond.Broadcast()
	}
	return nextEvents
}

func (s *LimitedConsumableAsyncBuffer[T]) GetAndRemoveNextEvent() Event[T] {
	e := s.ConsumableAsyncBuffer.GetAndRemoveNextEvent()
	s.cond.Broadcast()
	return e
}

func (i *Iterator[T]) HasNext() bool {
	return i.x < i.buffer.Len()
}

func (i *Iterator[T]) Next() Event[T] {
	e := i.buffer.Get(i.x)
	i.x++
	return e
}

func NewIterator[T any](buffer Buffer[T]) *Iterator[T] {
	return &Iterator[T]{
		x:      0,
		buffer: buffer,
	}
}
