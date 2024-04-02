package buffer

import (
	"fmt"
	"github.com/google/uuid"
	"go-stream-processing/events"
	"sync"
)

var defaultBufferCapacity = 5

type Reader[T any] interface {
	Get(i int) events.Event[T]
	Len() int
}

type basicBuffer[T any] []events.Event[T]

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

// AsyncBuffer represents an asynchronous buffer.
type AsyncBuffer[T any] struct {
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
	*AsyncBuffer[T]
}

// ConsumableAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations PeekNextEvent and RemoveNextEvent either return the next event,
// if any is available in the buffer or wait until next event is available.
type ConsumableAsyncBuffer[T any] struct {
	*AsyncBuffer[T]
	selectionPolicy SelectionPolicy[T]
}

func (b basicBuffer[T]) Get(i int) events.Event[T] {
	return b[i]
}

func (b basicBuffer[T]) Len() int {
	return len(b)
}

func NewAsyncBuffer[T any]() *AsyncBuffer[T] {
	s := &AsyncBuffer[T]{
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
		AsyncBuffer: NewAsyncBuffer[T](),
	}
	return s
}

func NewConsumableAsyncBuffer[T any](policy SelectionPolicy[T]) Buffer[T] {
	s := &ConsumableAsyncBuffer[T]{
		AsyncBuffer:     NewAsyncBuffer[T](),
		selectionPolicy: policy,
	}
	s.selectionPolicy.SetBuffer(&s.buffer)
	fmt.Printf("buff %p\n", s.buffer)
	return s
}

func (s *AsyncBuffer[T]) StartBlocking() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.stopped = false
}

func (s *AsyncBuffer[T]) StopBlocking() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.stopped = true
	s.cond.Broadcast()
}

func (s *AsyncBuffer[T]) Len() int {
	return len(s.buffer)
}

func (s *AsyncBuffer[T]) Get(i int) events.Event[T] {
	return s.buffer[i]
}

func (s *AsyncBuffer[T]) Dump() []events.Event[T] {
	destination := make([]events.Event[T], s.Len())
	copy(destination, s.buffer)
	return destination
}

// PeekNextEvent returns the next buffered event, but no event will be removed from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *AsyncBuffer[T]) PeekNextEvent() events.Event[T] {
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

func (s *AsyncBuffer[T]) GetAndRemoveNextEvent() events.Event[T] {
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
	return []events.Event[T]{s.AsyncBuffer.GetAndRemoveNextEvent()}
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
		selectionFound = s.selectionPolicy.NextSelectionReady()
		selection      EventSelection
	)

	fmt.Printf("1 getting from buffer(%v) %v %v \n", s.Len(), selectionFound, s.selectionPolicy.NextSelection())

	// Wait until the buffer has enough events
	for !selectionFound && !s.stopped {
		s.cond.Wait()
		fmt.Printf("1.5 getting from buffer %v %v \n", selectionFound, s.selectionPolicy.NextSelection())
		selectionFound = s.selectionPolicy.NextSelectionReady()
	}

	fmt.Printf("2 getting from buffer %v %v l:%v \n", selectionFound, s.selectionPolicy.NextSelection(), s.Len())

	selection = s.selectionPolicy.NextSelection()
	if selectionFound || selection.IsValid() {
		// Extract selected events from the buffer
		selectedEvents := s.buffer[selection.Start : selection.End+1]
		s.selectionPolicy.Shift()
		s.selectionPolicy.UpdateSelection()
		offset := s.selectionPolicy.NextSelection().Start
		fmt.Printf("!!offset %v %v\n", offset, s.selectionPolicy.NextSelection())
		s.buffer = s.buffer[offset:]
		fmt.Printf("!!offset2 %v %v\n", offset, s.selectionPolicy.NextSelection())
		s.selectionPolicy.Offset(offset)
		fmt.Printf("!!offset3 %v %v\n", offset, s.selectionPolicy.NextSelection())
		return selectedEvents
	}
	return make([]events.Event[T], 0)
}

func (s *ConsumableAsyncBuffer[T]) consume(selectionPolicy SelectionPolicy[T], selection EventSelection) {

	offset := selection.End

	fmt.Printf("Size before consume %v\n", s.Len())
	// Consume the selected events from the buffer
	s.buffer = s.buffer[offset+1:]

	if s.Len() > 0 {
		iter := newIterator[T](s)
		for iter.hasNext() && !selectionPolicy.NextSelectionReady() {
			selectionPolicy.UpdateSelection()
		}
	}
}

func (s *ConsumableAsyncBuffer[T]) AddEvents(events []events.Event[T]) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, events...)
	fmt.Printf("buff p:%p l:%v\n", s.buffer, s.buffer.Len())
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
