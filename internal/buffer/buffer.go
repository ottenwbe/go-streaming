package buffer

import (
	"fmt"
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/selection"

	"github.com/google/uuid"
)

var defaultBufferCapacity = 5

const LimitExceededFormat = "LimitedSimpleAsyncBuffer: Limit exceeded (%v > %v)"

// basicBuffer is an (internal) alias for an event array
type basicBuffer[T any] []events.Event[T]

// Buffer interface to interact with any buffer
type Buffer[T any] interface {
	GetAndConsumeNextEvents() []events.Event[T]
	PeekNextEvent() events.Event[T]
	AddEvent(event events.Event[T]) error
	AddEvents(events []events.Event[T]) error
	Len() int
	Get(x int) events.Event[T]
	Dump() []events.Event[T]
	StopBlocking()
	StartBlocking()
	GetAndRemoveNextEvent() events.Event[T]
}

type iterator[T any] struct {
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
// The Read operations GetNextEvent and RemoveNextEvent either return the next event,
// if any is available in the buffer or wait until next event is available.
type SimpleAsyncBuffer[T any] struct {
	*asyncBuffer[T]
}

// LimitedSimpleAsyncBuffer is a buffer with a fixed capacity limit.
type LimitedSimpleAsyncBuffer[T any] struct {
	*SimpleAsyncBuffer[T]
	limit int
}

// ConsumableAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations PeekNextEvent and RemoveNextEvent either return the next event,
// if any is available in the buffer or wait until next event is available based on a selection policy.
// see selection.Policy[T]
type ConsumableAsyncBuffer[T any] struct {
	*asyncBuffer[T]
	selectionPolicy selection.Policy[T]
}

func (b basicBuffer[T]) Get(i int) events.Event[T] {
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

// NewConsumableAsyncBuffer creates a new buffer that consumes events based on a selection policy.
func NewConsumableAsyncBuffer[T any](policy selection.Policy[T]) Buffer[T] {
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

func (s *asyncBuffer[T]) Get(i int) events.Event[T] {
	return s.buffer[i]
}

func (s *asyncBuffer[T]) Dump() []events.Event[T] {
	destination := make([]events.Event[T], s.Len())
	copy(destination, s.buffer)
	return destination
}

// PeekNextEvent returns the next buffered event, but no event will be removed from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *asyncBuffer[T]) PeekNextEvent() events.Event[T] {
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

func (s *asyncBuffer[T]) GetAndRemoveNextEvent() events.Event[T] {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	var e events.Event[T] = nil

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

// GetAndConsumeNextEvents returns the next event from the buffer and removes it.
func (s *SimpleAsyncBuffer[T]) GetAndConsumeNextEvents() []events.Event[T] {
	return []events.Event[T]{s.asyncBuffer.GetAndRemoveNextEvent()}
}

// AddEvents adds multiple events to the buffer.
func (s *SimpleAsyncBuffer[T]) AddEvents(events []events.Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, events...)
	s.cond.Broadcast()
	return nil
}

// AddEvent adds a single event to the buffer.
func (s *SimpleAsyncBuffer[T]) AddEvent(event events.Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, event)
	s.cond.Broadcast()
	return nil
}

// GetAndConsumeNextEvents returns the next buffered events and removes this event from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *ConsumableAsyncBuffer[T]) GetAndConsumeNextEvents() []events.Event[T] {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	var (
		selectedEvents basicBuffer[T]
		selectionFound = s.selectionPolicy.NextSelectionReady()
		selection      selection.EventSelection
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
		selectedEvents = make([]events.Event[T], len(src))
		copy(selectedEvents, src)
	}

	if selectionFound {
		s.selectionPolicy.Shift()
		s.selectionPolicy.UpdateSelection()
		offset := s.selectionPolicy.NextSelection().Start
		for i := range offset {
			s.buffer[i] = nil
		}
		s.buffer = s.buffer[offset:]
		s.selectionPolicy.Offset(offset)
	}

	return selectedEvents
}

// AddEvents adds multiple events to the buffer and updates the selection policy.
func (s *ConsumableAsyncBuffer[T]) AddEvents(events []events.Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, events...)
	s.selectionPolicy.UpdateSelection()

	s.cond.Broadcast()
	return nil
}

// AddEvent adds a single event to the buffer and updates the selection policy.
func (s *ConsumableAsyncBuffer[T]) AddEvent(event events.Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, event)
	s.selectionPolicy.UpdateSelection()

	s.cond.Broadcast()
	return nil
}

// AddEvents adds multiple events to the buffer, ensuring the limit is not exceeded.
func (s *LimitedSimpleAsyncBuffer[T]) AddEvents(events []events.Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	err := ensureLimit(s.limit, s.buffer.Len()+len(events))
	if err != nil {
		return err
	}

	s.buffer = append(s.buffer, events...)
	s.cond.Broadcast()
	return nil
}

// AddEvent adds a single event to the buffer, ensuring the limit is not exceeded.
func (s *LimitedSimpleAsyncBuffer[T]) AddEvent(event events.Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	err := ensureLimit(s.limit, s.buffer.Len()+1)
	if err != nil {
		return err
	}

	s.buffer = append(s.buffer, event)
	s.cond.Broadcast()
	return nil
}

func ensureLimit(limit, newBufferLen int) error {
	if limit < newBufferLen {
		return fmt.Errorf(LimitExceededFormat, newBufferLen, limit)
	}
	return nil
}

func (i *iterator[T]) hasNext() bool {
	return i.x < i.buffer.Len()
}

func (i *iterator[T]) next() events.Event[T] {
	e := i.buffer.Get(i.x)
	i.x++
	return e
}

func newIterator[T any](buffer Buffer[T]) *iterator[T] {
	return &iterator[T]{
		x:      0,
		buffer: buffer,
	}
}
