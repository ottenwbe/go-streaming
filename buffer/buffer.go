package buffer

import (
	"go-stream-processing/events"
	"sync"
)

var defaultBufferCapacity = 5

type Buffer interface {
	GetAndRemoveNextEvent() events.Event
	GetNextEvent() events.Event
	RemoveNextEvent()
	AddEvent(event events.Event)
	AddEvents(events []events.Event)
	Len() int
	Dump() []events.Event
}

// SimpleAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations GetNextEvent and RemoveNextEvent either return the next event,
// if any is available in the buffer or wait until next event is available.
type SimpleAsyncBuffer struct {
	buffer      []events.Event
	bufferMutex sync.Mutex
	cond        *sync.Cond
}

func NewAsyncBuffer() Buffer {
	s := &SimpleAsyncBuffer{
		buffer:      make([]events.Event, 0, defaultBufferCapacity),
		bufferMutex: sync.Mutex{},
	}
	s.cond = sync.NewCond(&s.bufferMutex)
	return s
}

func (s *SimpleAsyncBuffer) getNextEvent() events.Event {
	return s.buffer[0]
}

func (s *SimpleAsyncBuffer) removeNextEvent() {
	s.buffer = s.buffer[1:]
}

func (s *SimpleAsyncBuffer) GetAndRemoveNextEvent() events.Event {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.Len() == 0 {
		s.cond.Wait()
	}

	e := s.getNextEvent()
	s.removeNextEvent()

	return e
}

func (s *SimpleAsyncBuffer) GetNextEvent() events.Event {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()
	if s.Len() == 0 {
		s.cond.Wait()
	}
	return s.getNextEvent()
}

func (s *SimpleAsyncBuffer) RemoveNextEvent() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.Len() > 0 {
		s.removeNextEvent()
	}
}

func (s *SimpleAsyncBuffer) AddEvents(events []events.Event) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, events...)
	s.cond.Signal()
}

func (s *SimpleAsyncBuffer) AddEvent(event events.Event) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.buffer = append(s.buffer, event)
	s.cond.Signal()
}

func (s *SimpleAsyncBuffer) Dump() []events.Event {
	return s.buffer
}

func (s *SimpleAsyncBuffer) Len() int {

	return len(s.buffer)
}
