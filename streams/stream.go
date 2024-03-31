package streams

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
	"go.uber.org/zap"
	"sync"
)

type StreamID uuid.UUID

func (s StreamID) isNil() bool {
	return s == StreamID(uuid.Nil)
}

func (s StreamID) String() string {
	return uuid.UUID(s).String()
}

type NotificationMap[T any] map[StreamReceiverID]events.EventChannel[T]

func (m NotificationMap[T]) notifyAll(events []events.Event[T]) {
	for _, e := range events {
		m.notify(e)
	}
}

func (m NotificationMap[T]) notify(e events.Event[T]) {
	for id, notifier := range m {
		func() {
			defer func() {
				if r := recover(); r != nil {
					zap.S().Infof("recovered notify panic %v", id)
				}
			}()
			notifier <- e
		}()
	}
}

type Stream[T any] interface {
	Start()
	Stop()

	ID() StreamID
	Name() string
	Description() StreamDescription

	Publish(events.Event[T]) error
	Subscribe() *StreamReceiver[T]
	Unsubscribe(id StreamReceiverID)

	setNotifiers(notificationMap NotificationMap[T])
	notifiers() NotificationMap[T]
	events() buffer.Buffer[T]
	setEvents(buffer.Buffer[T])
}

type LocalSyncStream[T any] struct {
	description StreamDescription

	notify      NotificationMap[T]
	notifyMutex sync.Mutex
}

type LocalAsyncStream[T any] struct {
	description StreamDescription

	inChannel events.EventChannel[T]
	buffer    buffer.Buffer[T]

	notify      NotificationMap[T]
	notifyMutex sync.Mutex

	active bool
}

// NewLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func NewLocalSyncStream[T any](description StreamDescription) *LocalSyncStream[T] {
	return &LocalSyncStream[T]{
		description: description,
		notify:      make(NotificationMap[T]),
	}
}

// NewLocalAsyncStream is created w/ event buffering
func NewLocalAsyncStream[T any](description StreamDescription) *LocalAsyncStream[T] {
	a := &LocalAsyncStream[T]{
		description: description,
		active:      false,
		buffer:      buffer.NewSimpleAsyncBuffer[T](),
		notify:      make(NotificationMap[T]),
	}
	return a
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

func (l *LocalAsyncStream[T]) Stop() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	l.active = false
	close(l.inChannel)
	for _, c := range l.notify {
		close(c)
	}
	l.buffer.StopBlocking()
}

func (l *LocalAsyncStream[T]) Start() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if !l.active {

		l.active = true
		l.inChannel = make(chan events.Event[T])

		// read input from in Channel and buffer it
		go func() {
			for {
				event, more := <-l.inChannel
				if more {
					l.buffer.AddEvent(event)
				} else {
					return
				}
			}
		}()

		// read buffer and notify
		go func() {
			for l.active {
				l.notify.notifyAll(l.buffer.GetAndConsumeNextEvents())
			}
		}()
	}
}

func (l *LocalAsyncStream[T]) Description() StreamDescription {
	return l.description
}

func (l *LocalAsyncStream[T]) ID() StreamID {
	return l.description.StreamID()
}

func (l *LocalAsyncStream[T]) Name() string {
	return l.description.Name
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

func (l *LocalAsyncStream[T]) Subscribe() *StreamReceiver[T] {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	rec := NewStreamReceiver[T](l)

	l.notify[rec.ID] = rec.Notify

	return rec
}

func (s *LocalSyncStream[T]) Description() StreamDescription {
	return s.description
}

func (s *LocalSyncStream[T]) ID() StreamID {
	return s.description.StreamID()
}

func (s *LocalSyncStream[T]) Name() string {
	return s.description.Name
}

func (s *LocalSyncStream[T]) Publish(e events.Event[T]) error {
	s.notify.notify(e)
	return nil
}

func (l *LocalAsyncStream[T]) Unsubscribe(id StreamReceiverID) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if c, ok := l.notify[id]; ok {
		delete(l.notify, id)
		close(c)
	}
}

func (s *LocalSyncStream[T]) Unsubscribe(id StreamReceiverID) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	if c, ok := s.notify[id]; ok {
		delete(s.notify, id)
		close(c)
	}
}

func (s *LocalSyncStream[T]) Subscribe() *StreamReceiver[T] {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	rec := NewStreamReceiver[T](s)
	s.notify[rec.ID] = rec.Notify

	return rec
}

func (s *LocalSyncStream[T]) Start() {

}

func (s *LocalSyncStream[T]) Stop() {
	for _, c := range s.notify {
		close(c)
	}
}

func (s *LocalSyncStream[T]) setNotifiers(m NotificationMap[T]) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()
	s.notify = m
}

func (s *LocalSyncStream[T]) notifiers() NotificationMap[T] {
	return s.notify
}

func (l *LocalAsyncStream[T]) setNotifiers(m NotificationMap[T]) {
	l.notify = m
}

func (l *LocalAsyncStream[T]) notifiers() NotificationMap[T] {
	return l.notify
}

var (
	StreamInactive error
)

func init() {
	StreamInactive = errors.New("stream not active")
}
