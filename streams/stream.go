package streams

import (
	"github.com/google/uuid"
	buffer2 "go-stream-processing/buffer"
	"go-stream-processing/events"
)

type Stream interface {
	Start()
	Stop()

	Name() string
	ID() uuid.UUID

	Publish(events.Event)
	Subscribe(rec StreamReceiver)

	setNotifiers(notificationMap NotificationMap)
	notifiers() NotificationMap
}

type NotificationMap map[uuid.UUID]chan events.Event

type LocalSyncStream struct {
	name   string
	id     uuid.UUID
	notify NotificationMap
	active bool
}

type LocalAsyncStream struct {
	name      string
	id        uuid.UUID
	inChannel chan events.Event
	buffer    buffer2.Buffer
	notify    NotificationMap
	active    bool
	done      chan bool
}

// NewLocalSyncStream is created w/o event buffering
func NewLocalSyncStream(name string) *LocalSyncStream {
	return &LocalSyncStream{
		name:   name,
		id:     uuid.New(),
		notify: make(NotificationMap),
		active: true,
	}
}

// NewLocalAsyncStream is created w/ event buffering
func NewLocalAsyncStream(name string) *LocalAsyncStream {
	a := &LocalAsyncStream{
		name:      name,
		id:        uuid.New(),
		inChannel: make(chan events.Event),
		active:    true,
		buffer:    buffer2.NewBuffer(),
		notify:    make(NotificationMap),
	}
	a.Start()
	return a
}

func (l *LocalAsyncStream) Stop() {
	l.active = false
	close(l.inChannel)
}

func (l *LocalAsyncStream) Start() {

	// read input from in Channel and buffer it
	go func() {
		for {
			event, more := <-l.inChannel
			if more {
				l.buffer.AddEvent(event)
			} else {
				l.done <- true
				return
			}
		}
	}()

	// read buffer and notify
	go func() {
		for l.active {
			if l.buffer.Len() > 0 {
				for _, notifier := range l.notify {
					notifier <- l.buffer.GetAndRemoveNextEvent()
				}
			}
		}
	}()

}

func (l *LocalAsyncStream) Name() string {
	return l.name
}

func (l *LocalAsyncStream) ID() uuid.UUID {
	return l.id
}

func (l *LocalAsyncStream) Publish(event events.Event) {
	l.inChannel <- event
}

func (l *LocalAsyncStream) Subscribe(rec StreamReceiver) {
	l.notify[rec.ID] = rec.Notify
}

func (s *LocalSyncStream) Name() string {
	return s.name
}

func (s *LocalSyncStream) ID() uuid.UUID {
	return s.id
}

func (s *LocalSyncStream) Publish(e events.Event) {
	for _, notifier := range s.notify {
		notifier <- e
	}
}

func (s *LocalSyncStream) Subscribe(rec StreamReceiver) {
	s.notify[rec.ID] = rec.Notify
}

func (s *LocalSyncStream) Start() {
	s.active = true
}

func (s *LocalSyncStream) Stop() {
	s.active = false
}

func (s *LocalSyncStream) setNotifiers(m NotificationMap) {
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
