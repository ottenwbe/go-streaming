package buffer

import (
	"github.com/google/uuid"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/selection"
	"sync"
)

var defaultBufferCapacity = 5

// basicBuffer is an (internal) alias for an event array
type basicBuffer[T any] []events.Event[T]

// Buffer interface to interact with any buffer
type Buffer[T any] interface {
	GetAndConsumeNextEvents() []events.Event[T]
	PeekNextEvent() events.Event[T]
	AddEvent(event events.Event[T])
	AddEvents(events []events.Event[T])
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

// ConsumableAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations PeekNextEvent and RemoveNextEvent either return the next event,
// if any is available in the buffer or wait until next event is available.
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

func NewSimpleAsyncBuffer[T any]() Buffer[T] {
	s := &SimpleAsyncBuffer[T]{
		asyncBuffer: newAsyncBuffer[T](),
	}
	return s
}

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
		s.buffer = s.buffer[1:]
		return e
	}

	return nil
}

func (s *SimpleAsyncBuffer[T]) GetAndConsumeNextEvents() []events.Event[T] {
	return []events.Event[T]{s.asyncBuffer.GetAndRemoveNextEvent()}
}

func (s *SimpleAsyncBuffer[T]) AddEvents(events []events.Event[T]) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, events...)
	s.cond.Broadcast()
}

func (s *SimpleAsyncBuffer[T]) AddEvent(event events.Event[T]) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, event)
	s.cond.Broadcast()
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
		selectedEvents = s.buffer[selection.Start : selection.End+1]
	}

	if selectionFound {
		s.selectionPolicy.Shift()
		s.selectionPolicy.UpdateSelection()
		offset := s.selectionPolicy.NextSelection().Start
		s.buffer = s.buffer[offset:]
		s.selectionPolicy.Offset(offset)
	}

	return selectedEvents
}

func (s *ConsumableAsyncBuffer[T]) AddEvents(events []events.Event[T]) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, events...)
	s.selectionPolicy.UpdateSelection()

	s.cond.Broadcast()
}

func (s *ConsumableAsyncBuffer[T]) AddEvent(event events.Event[T]) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, event)
	s.selectionPolicy.UpdateSelection()

	s.cond.Broadcast()
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
