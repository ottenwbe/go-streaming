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

func (s StreamID) String() string {
	return uuid.UUID(s).String()
}

type NotificationMap map[StreamReceiverID]chan events.Event

func (m NotificationMap) notify(e events.Event) {
	for id, notifier := range m {
		func() {
			defer func() {
				if r := recover(); r != nil {
					zap.S().Infof("Recovered Notify Panic %v", id)
				}
			}()
			notifier <- e
		}()
	}
}

type Stream interface {
	Start()
	Stop()

	Name() string
	ID() StreamID

	Publish(events.Event) error
	Subscribe() *StreamReceiver
	Unsubscribe(id StreamReceiverID)

	setNotifiers(notificationMap NotificationMap)
	notifiers() NotificationMap
	events() buffer.Buffer
	setEvents(buffer.Buffer)
}

type LocalSyncStream struct {
	name string
	id   StreamID

	notify      NotificationMap
	notifyMutex sync.Mutex

	active bool
}

type LocalAsyncStream struct {
	name string
	id   StreamID

	inChannel chan events.Event
	buffer    buffer.Buffer
	done      chan bool
	runner    *localAsyncStreamRunner

	notify      NotificationMap
	notifyMutex sync.Mutex

	active bool
}

// NewLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func NewLocalSyncStream(name string, streamID StreamID) *LocalSyncStream {
	return &LocalSyncStream{
		name:   name,
		id:     streamID,
		notify: make(NotificationMap),
		active: false,
	}
}

// NewLocalAsyncStream is created w/ event buffering
func NewLocalAsyncStream(name string, streamID StreamID) *LocalAsyncStream {
	a := &LocalAsyncStream{
		name:      name,
		id:        streamID,
		inChannel: make(chan events.Event),
		active:    false,
		buffer:    buffer.NewAsyncBuffer(),
		notify:    make(NotificationMap),
		runner:    &localAsyncStreamRunner{active: false},
	}
	return a
}

func (s *LocalSyncStream) events() buffer.Buffer {
	return buffer.NewAsyncBuffer()
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
	l.active = false
}

type localAsyncStreamRunner struct {
	active bool
	aMutex sync.Mutex
}

func (l *localAsyncStreamRunner) isActive() bool {
	return l.active
}

func (l *localAsyncStreamRunner) asyncBufferChannelEvents(in chan events.Event, buffer buffer.Buffer) {
	l.aMutex.Lock()
	if l.active {
		return
	}
	go func() {
		l.active = true
		l.aMutex.Unlock()
		for {
			event, more := <-in
			if more {
				buffer.AddEvent(event)
			} else {
				l.active = false
				return
			}
		}
	}()
}

func (l *LocalAsyncStream) Start() {

	if !l.active {

		l.active = true

		// read input from in Channel and buffer it
		if !l.runner.isActive() {
			l.runner.asyncBufferChannelEvents(l.inChannel, l.buffer)
		}

		// read buffer and notify
		go func() {
			for l.active {
				l.notify.notify(l.buffer.GetAndRemoveNextEvent())
			}
		}()
	}
}

func (l *LocalAsyncStream) Name() string {
	return l.name
}

func (l *LocalAsyncStream) ID() StreamID {
	return l.id
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

func (s *LocalSyncStream) Name() string {
	return s.name
}

func (s *LocalSyncStream) ID() StreamID {
	return s.id
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
