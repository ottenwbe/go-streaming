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

type NotificationMap map[StreamReceiverID]events.EventChannel

func (m NotificationMap) notifyAll(events []events.Event) {
	for _, e := range events {
		m.notify(e)
	}
}

func (m NotificationMap) notify(e events.Event) {
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

type Stream interface {
	Start()
	Stop()

	ID() StreamID
	Name() string
	Description() StreamDescription

	Publish(events.Event) error
	Subscribe() *StreamReceiver
	Unsubscribe(id StreamReceiverID)

	setNotifiers(notificationMap NotificationMap)
	notifiers() NotificationMap
	events() buffer.Buffer
	setEvents(buffer.Buffer)
}

type LocalSyncStream struct {
	description StreamDescription

	notify      NotificationMap
	notifyMutex sync.Mutex

	active bool
}

type LocalAsyncStream struct {
	description StreamDescription

	inChannel events.EventChannel
	buffer    buffer.Buffer

	notify      NotificationMap
	notifyMutex sync.Mutex

	active bool
}

// NewLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func NewLocalSyncStream(description StreamDescription) *LocalSyncStream {
	return &LocalSyncStream{
		description: description,
		notify:      make(NotificationMap),
		active:      false,
	}
}

// NewLocalAsyncStream is created w/ event buffering
func NewLocalAsyncStream(description StreamDescription) *LocalAsyncStream {
	a := &LocalAsyncStream{
		description: description,
		active:      false,
		buffer:      buffer.NewSimpleAsyncBuffer(),
		notify:      make(NotificationMap),
	}
	return a
}

func (s *LocalSyncStream) events() buffer.Buffer {
	return buffer.NewSimpleAsyncBuffer()
}

func (s *LocalSyncStream) setEvents(buffer.Buffer) {
	// Intentional Event Loss
}

func (l *LocalAsyncStream) events() buffer.Buffer {
	return l.buffer
}

func (l *LocalAsyncStream) setEvents(b buffer.Buffer) {
	l.buffer.AddEvents(b.Dump())
}

func (l *LocalAsyncStream) Stop() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	l.active = false
	close(l.inChannel)
	for _, c := range l.notify {
		close(c)
	}
	l.buffer.StopBlocking()
}

func (l *LocalAsyncStream) Start() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if !l.active {

		l.active = true
		l.inChannel = make(chan events.Event)

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

func (l *LocalAsyncStream) Description() StreamDescription {
	return l.description
}

func (l *LocalAsyncStream) ID() StreamID {
	return l.description.StreamID()
}

func (l *LocalAsyncStream) Name() string {
	return l.description.Name
}

func (l *LocalAsyncStream) Publish(event events.Event) error {
	// Handle stream inactive error
	if !l.active {
		return StreamInactive
	}
	// Publish event...
	l.inChannel <- event
	return nil
}

func (l *LocalAsyncStream) Subscribe() *StreamReceiver {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	rec := NewStreamReceiver(l)

	l.notify[rec.ID] = rec.Notify

	return rec
}

func (s *LocalSyncStream) Description() StreamDescription {
	return s.description
}

func (s *LocalSyncStream) ID() StreamID {
	return s.description.StreamID()
}

func (s *LocalSyncStream) Name() string {
	return s.description.Name
}

func (s *LocalSyncStream) Publish(e events.Event) error {
	if !s.active {
		return StreamInactive
	}

	s.notify.notify(e)
	return nil
}

func (l *LocalAsyncStream) Unsubscribe(id StreamReceiverID) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if c, ok := l.notify[id]; ok {
		delete(l.notify, id)
		close(c)
	}
}

func (s *LocalSyncStream) Unsubscribe(id StreamReceiverID) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	if c, ok := s.notify[id]; ok {
		delete(s.notify, id)
		close(c)
	}
}

func (s *LocalSyncStream) Subscribe() *StreamReceiver {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	rec := NewStreamReceiver(s)
	s.notify[rec.ID] = rec.Notify

	return rec
}

func (s *LocalSyncStream) Start() {
	s.active = true
}

func (s *LocalSyncStream) Stop() {
	s.active = false
	for _, c := range s.notify {
		close(c)
	}
}

func (s *LocalSyncStream) setNotifiers(m NotificationMap) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()
	s.notify = m
}

func (s *LocalSyncStream) notifiers() NotificationMap {
	return s.notify
}

func (l *LocalAsyncStream) setNotifiers(m NotificationMap) {
	l.notify = m
}

func (l *LocalAsyncStream) notifiers() NotificationMap {
	return l.notify
}

var (
	StreamInactive error
)

func init() {
	StreamInactive = errors.New("stream not active")
}
