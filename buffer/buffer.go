package buffer

import (
	"go-stream-processing/events"
	"sync"
)

var defaultBufferCapacity = 5

type Buffer interface {
	GetAndConsumeNextEvents() []events.Event
	PeekNextEvent() events.Event
	AddEvent(event events.Event)
	AddEvents(events []events.Event)
	Len() int
	get(x int) events.Event
	Dump() []events.Event
	StopBlocking()
	StartBlocking()
}

type iterator struct {
	x      int
	buffer Buffer
}

// AsyncBuffer represents an asynchronous buffer.
type AsyncBuffer struct {
	buffer      []events.Event
	bufferMutex sync.Mutex
	cond        *sync.Cond
	stopped     bool
}

// SimpleAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations GetNextEvent and RemoveNextEvent either return the next event,
// if any is available in the buffer or wait until next event is available.
type SimpleAsyncBuffer struct {
	*AsyncBuffer
}

// ConsumableAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations PeekNextEvent and RemoveNextEvent either return the next event,
// if any is available in the buffer or wait until next event is available.
type ConsumableAsyncBuffer struct {
	*AsyncBuffer
	selectionPolicy SelectionPolicy
}

func NewAsyncBuffer() *AsyncBuffer {
	s := &AsyncBuffer{
		buffer:      make([]events.Event, 0, defaultBufferCapacity),
		bufferMutex: sync.Mutex{},
		stopped:     false,
	}
	s.cond = sync.NewCond(&s.bufferMutex)
	return s
}

func NewSimpleAsyncBuffer() Buffer {
	s := &SimpleAsyncBuffer{
		AsyncBuffer: NewAsyncBuffer(),
	}
	return s
}

func NewConsumableAsyncBuffer(policy SelectionPolicy) Buffer {
	s := &ConsumableAsyncBuffer{
		AsyncBuffer:     NewAsyncBuffer(),
		selectionPolicy: policy,
	}
	return s
}

func (s *AsyncBuffer) StartBlocking() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.stopped = false
}

func (s *AsyncBuffer) StopBlocking() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.stopped = true
	s.cond.Broadcast()
}

func (s *AsyncBuffer) Len() int {
	return len(s.buffer)
}

func (s *AsyncBuffer) get(i int) events.Event {
	return s.buffer[i]
}

func (s *AsyncBuffer) Dump() []events.Event {
	destination := make([]events.Event, s.Len())
	copy(destination, s.buffer)
	return destination
}

// PeekNextEvent returns the next buffered event, but no event will be removed from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *AsyncBuffer) PeekNextEvent() events.Event {
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

func (s *SimpleAsyncBuffer) GetAndConsumeNextEvents() []events.Event {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	var e events.Event = nil

	if s.Len() == 0 && !s.stopped {
		s.cond.Wait()
	}

	if s.Len() > 0 {
		e = s.buffer[0]
		s.buffer = s.buffer[1:]
		return []events.Event{e}
	}

	return []events.Event{}
}

func (s *SimpleAsyncBuffer) AddEvents(events []events.Event) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, events...)
	s.cond.Broadcast()
}

func (s *SimpleAsyncBuffer) AddEvent(event events.Event) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, event)
	s.cond.Broadcast()
}

// GetAndConsumeNextEvents returns the next buffered events and removes this event from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *ConsumableAsyncBuffer) GetAndConsumeNextEvents() []events.Event {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	var (
		selectionFound = s.selectionPolicy.NextSelectionReady()
		selection      EventRange
	)

	// Wait until the buffer has enough events
	for !selectionFound && !s.stopped {
		s.cond.Wait()
		selectionFound = s.selectionPolicy.NextSelectionReady()
	}

	if selectionFound {
		selection = s.selectionPolicy.NextSelection()
		// Extract selected events from the buffer
		selectedEvents := s.buffer[selection.Start : selection.End+1]
		s.consume(s.selectionPolicy, selection)

		return selectedEvents
	}
	return make([]events.Event, 0)
}

func (s *ConsumableAsyncBuffer) consume(selectionPolicy SelectionPolicy, selection EventRange) {
	// Consume the selected events from the buffer
	s.buffer = s.buffer[selection.End+1:]

	selectionPolicy.ShiftWithOffset(selection.End + 1)
	if s.Len() > 0 {
		iter := newIterator(s)
		for iter.hasNext() && !selectionPolicy.NextSelectionReady() {
			selectionPolicy.UpdateSelection(iter.next())
		}
	}
}

func (s *ConsumableAsyncBuffer) AddEvents(events []events.Event) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	for _, event := range events {
		s.selectionPolicy.UpdateSelection(event)
	}
	s.buffer = append(s.buffer, events...)
	s.cond.Broadcast()
}

func (s *ConsumableAsyncBuffer) AddEvent(event events.Event) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.selectionPolicy.UpdateSelection(event)
	s.buffer = append(s.buffer, event)
	s.cond.Broadcast()
}

func (i *iterator) hasNext() bool {
	return i.x < i.buffer.Len()
}

func (i *iterator) next() events.Event {
	e := i.buffer.get(i.x)
	i.x++
	return e
}

func newIterator(buffer Buffer) *iterator {
	return &iterator{
		x:      0,
		buffer: buffer,
	}
}
