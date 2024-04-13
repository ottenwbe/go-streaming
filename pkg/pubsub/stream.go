package pubsub

import (
	"errors"
	"go-stream-processing/internal/buffer"
	"go-stream-processing/pkg/events"
	"go.uber.org/zap"
	"sync"
)

type StreamID string //uuid.UUID

func (s StreamID) isNil() bool {
	return s == StreamID("")
}

func (s StreamID) String() string {
	return string(s)
}

type notificationMap[T any] map[StreamReceiverID]events.EventChannel[T]

func (m notificationMap[T]) notifyAll(events []events.Event[T]) {
	for _, e := range events {
		m.notify(e)
	}
}

func (m notificationMap[T]) notify(e events.Event[T]) {
	for id, notifier := range m {
		func() {
			defer func() {
				if r := recover(); r != nil {
					zap.S().Debugf("recovered notify panic for stream %v", id)
				}
			}()
			notifier <- e
		}()
	}
}

type Publisher[T any] interface {
	Publish(events.Event[T]) error
	ID() StreamID
}

type StreamControl interface {
	Run()
	TryClose()

	HasPublishersOrSubscribers() bool

	ID() StreamID
	Description() StreamDescription
}

type Stream[T any] interface {
	StreamControl

	Publish(events.Event[T]) error
	subscribe() *StreamReceiver[T]
	unsubscribe(id StreamReceiverID)

	setNotifiers(notificationMap notificationMap[T])
	notifiers() notificationMap[T]
	events() buffer.Buffer[T]
	setEvents(buffer.Buffer[T])
}

type LocalSyncStream[T any] struct {
	description StreamDescription

	notify      notificationMap[T]
	notifyMutex sync.Mutex
}

type LocalAsyncStream[T any] struct {
	description StreamDescription

	inChannel events.EventChannel[T]
	buffer    buffer.Buffer[T]

	notify      notificationMap[T]
	notifyMutex sync.Mutex

	active    bool
	closeChan chan bool
}

// NewLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func NewLocalSyncStream[T any](description StreamDescription) *LocalSyncStream[T] {
	return &LocalSyncStream[T]{
		description: description,
		notify:      make(notificationMap[T]),
	}
}

// NewLocalAsyncStream is created w/ event buffering
func NewLocalAsyncStream[T any](description StreamDescription) *LocalAsyncStream[T] {
	a := &LocalAsyncStream[T]{
		description: description,
		active:      false,
		notify:      make(notificationMap[T]),
		closeChan:   make(chan bool),
	}
	return a
}

func (s *LocalSyncStream[T]) HasPublishersOrSubscribers() bool {
	return len(s.notify) > 0
}

func (l *LocalAsyncStream[T]) HasPublishersOrSubscribers() bool {
	return len(l.notify) > 0
}

func (s *LocalSyncStream[T]) events() buffer.Buffer[T] {
	return buffer.NewSimpleAsyncBuffer[T]()
}

func (s *LocalSyncStream[T]) setEvents(buffer.Buffer[T]) {
	// Intentional Event Loss
}

func (l *LocalAsyncStream[T]) events() buffer.Buffer[T] {
	return l.buffer
}

func (l *LocalAsyncStream[T]) setEvents(b buffer.Buffer[T]) {
	l.buffer.AddEvents(b.Dump())
}

func (l *LocalAsyncStream[T]) TryClose() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if len(l.notify) == 0 && l.active {
		l.active = false
		close(l.inChannel)
		l.buffer.StopBlocking()

		<-l.closeChan
		<-l.closeChan
	}
}

func (l *LocalAsyncStream[T]) Run() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if !l.active {

		l.active = true
		l.inChannel = make(chan events.Event[T])
		l.buffer = buffer.NewSimpleAsyncBuffer[T]()

		// read input from in Channel and buffer it
		go func() {
			for {
				event, more := <-l.inChannel
				if more {
					l.buffer.AddEvent(event)
				} else {
					l.closeChan <- true
					return
				}
			}
		}()

		// read buffer and notify
		go func() {
			for l.active {
				l.notify.notifyAll(l.buffer.GetAndConsumeNextEvents())
			}
			l.closeChan <- true
		}()

	}
}

func (l *LocalAsyncStream[T]) Description() StreamDescription {
	return l.description
}

func (l *LocalAsyncStream[T]) ID() StreamID {
	return l.description.StreamID()
}

func (l *LocalAsyncStream[T]) Publish(event events.Event[T]) error {
	// Handle stream inactive error
	if !l.active {
		return StreamInactive
	}
	// Publish event...
	l.inChannel <- event
	return nil
}

func (l *LocalAsyncStream[T]) subscribe() *StreamReceiver[T] {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if l.active {
		rec := NewStreamReceiver[T](l)
		l.notify[rec.ID] = rec.Notify
		return rec
	}

	return nil
}

func (s *LocalSyncStream[T]) Description() StreamDescription {
	return s.description
}

func (s *LocalSyncStream[T]) ID() StreamID {
	return s.description.StreamID()
}

func (s *LocalSyncStream[T]) Publish(e events.Event[T]) error {
	s.notify.notify(e)
	return nil
}

func (l *LocalAsyncStream[T]) unsubscribe(id StreamReceiverID) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if c, ok := l.notify[id]; ok {
		delete(l.notify, id)
		close(c)
	}
}

func (s *LocalSyncStream[T]) unsubscribe(id StreamReceiverID) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	if c, ok := s.notify[id]; ok {
		delete(s.notify, id)
		close(c)
	}
}

func (s *LocalSyncStream[T]) subscribe() *StreamReceiver[T] {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	rec := NewStreamReceiver[T](s)
	s.notify[rec.ID] = rec.Notify

	return rec
}

func (s *LocalSyncStream[T]) Run() {

}

func (s *LocalSyncStream[T]) TryClose() {

}

func (s *LocalSyncStream[T]) setNotifiers(m notificationMap[T]) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()
	s.notify = m
}

func (s *LocalSyncStream[T]) notifiers() notificationMap[T] {
	return s.notify
}

func (l *LocalAsyncStream[T]) setNotifiers(m notificationMap[T]) {
	l.notify = m
}

func (l *LocalAsyncStream[T]) notifiers() notificationMap[T] {
	return l.notify
}

var (
	StreamInactive error
)

func init() {
	StreamInactive = errors.New("stream not active")
}
