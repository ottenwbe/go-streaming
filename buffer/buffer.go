package buffer

import (
	"go-stream-processing/events"
	"sync"
)

type Buffer interface {
	GetAndRemoveNextEvent() events.Event
	GetNextEvent() events.Event
	RemoveNextEvent()
	AddEvent(event events.Event)
	Len() int
}

type SimpleBuffer struct {
	buffer []events.Event
	mutex  sync.Mutex
}

type SimpleAsyncBuffer struct {
	Buffer        Buffer
	selectionChan chan events.Event
	isWaiting     bool
	mutex         sync.Mutex
}

func NewBuffer() Buffer {
	return &SimpleBuffer{
		buffer: make([]events.Event, 0),
	}
}

func NewAsyncBuffer() Buffer {
	return &SimpleAsyncBuffer{
		Buffer:        NewBuffer(),
		isWaiting:     false,
		selectionChan: make(chan events.Event),
	}
}

func (s *SimpleAsyncBuffer) GetAndRemoveNextEvent() events.Event {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	e := s.GetNextEvent()
	s.Buffer.RemoveNextEvent()
	return e
}

func (s *SimpleAsyncBuffer) GetNextEvent() events.Event {
	s.mutex.Lock()
	if s.Len() == 0 {
		s.isWaiting = true
		s.mutex.Unlock()
		return <-s.selectionChan
	} else {
		defer s.mutex.Unlock()
		return s.GetNextEvent()
	}

}

func (s *SimpleAsyncBuffer) RemoveNextEvent() {
	s.RemoveNextEvent()
}

func (s *SimpleAsyncBuffer) AddEvent(event events.Event) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isWaiting {
		s.selectionChan <- event
		s.isWaiting = false
	} else {
		s.AddEvent(event)
	}
}

func (s *SimpleAsyncBuffer) Len() int {
	return s.Len()
}

func (s *SimpleBuffer) GetAndRemoveNextEvent() events.Event {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	e := s.GetNextEvent()
	s.removeNextEvent()
	return e
}

func (s *SimpleBuffer) GetNextEvent() events.Event {
	return s.buffer[0]
}

func (s *SimpleBuffer) RemoveNextEvent() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.removeNextEvent()
}

func (s *SimpleBuffer) removeNextEvent() {
	s.buffer = s.buffer[1:]
}

func (s *SimpleBuffer) AddEvent(event events.Event) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.buffer = append(s.buffer, event)
}

func (s *SimpleBuffer) Len() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.buffer)
}
