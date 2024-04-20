package pubsub

import (
	"errors"
	"go-stream-processing/internal/buffer"
	"go-stream-processing/pkg/events"
	"sync"
)

func StreamInactive() error {
	return streamInactive
}

type notificationMap[T any] map[StreamReceiverID]events.EventChannel[T]

func (m notificationMap[T]) notifyAll(events []events.Event[T]) {
	for _, e := range events {
		m.notify(e)
	}
}

func (m notificationMap[T]) notify(e events.Event[T]) {
	for _, notifier := range m {

		/*
			The code should never panic here, because notifiers are unsubscribed before stream closes.
			However, if the concept changes, consider to handle the panic here:

			defer func() {
				if r := recover(); r != nil {
					zap.S().Debugf("recovered subscriberMap panic for stream %v", id)
				}
			}()*/
		notifier <- e
	}
}

type Publisher[T any] interface {
	Publish(events.Event[T]) error
	ID() StreamID
}

type StreamControl interface {
	Run()
	TryClose()
	ForceClose()

	HasPublishersOrSubscribers() bool

	ID() StreamID
	Description() StreamDescription
}

type Stream[T any] interface {
	StreamControl

	Publish(events.Event[T]) error

	subscribe() (*StreamReceiver[T], error)
	unsubscribe(id StreamReceiverID)
	setNotifiers(notificationMap notificationMap[T])
	notifiers() notificationMap[T]

	events() buffer.Buffer[T]
	setEvents(buffer.Buffer[T])
}

type LocalSyncStream[T any] struct {
	description StreamDescription

	subscriberMap notificationMap[T]
	notifyMutex   sync.Mutex
}

type LocalAsyncStream[T any] struct {
	description StreamDescription

	inChannel events.EventChannel[T]
	buffer    buffer.Buffer[T]

	subscriberMap notificationMap[T]
	notifyMutex   sync.Mutex

	active bool
	closed sync.WaitGroup
}

func NewStream[T any](id StreamID, async bool) Stream[T] {
	return NewStreamD[T](MakeStreamDescription(id, async))
}

func NewStreamD[T any](description StreamDescription) Stream[T] {
	var stream Stream[T]
	if description.Async {
		stream = NewLocalAsyncStream[T](description)
	} else {
		stream = NewLocalSyncStream[T](description)
	}
	return stream
}

// NewLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func NewLocalSyncStream[T any](description StreamDescription) *LocalSyncStream[T] {
	return &LocalSyncStream[T]{
		description:   description,
		subscriberMap: make(notificationMap[T]),
	}
}

// NewLocalAsyncStream is created w/ event buffering
func NewLocalAsyncStream[T any](description StreamDescription) *LocalAsyncStream[T] {
	a := &LocalAsyncStream[T]{
		description:   description,
		active:        false,
		subscriberMap: make(notificationMap[T]),
	}
	return a
}

func (s *LocalSyncStream[T]) HasPublishersOrSubscribers() bool {
	return len(s.subscriberMap) > 0
}

func (l *LocalAsyncStream[T]) HasPublishersOrSubscribers() bool {
	return len(l.subscriberMap) > 0
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

func (l *LocalAsyncStream[T]) ForceClose() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	for id := range l.subscriberMap {
		l.doUnsubscribe(id)
	}
	l.doClose(true)
}

func (l *LocalAsyncStream[T]) TryClose() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	l.doClose(false)
}

func (l *LocalAsyncStream[T]) doClose(force bool) {
	if (len(l.subscriberMap) == 0 || force) && l.active {
		l.active = false
		close(l.inChannel)
		l.buffer.StopBlocking()

		l.closed.Wait()
	}
}

func (l *LocalAsyncStream[T]) Run() {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if !l.active {

		l.active = true
		l.inChannel = make(chan events.Event[T])
		l.buffer = buffer.NewSimpleAsyncBuffer[T]()
		l.closed = sync.WaitGroup{}
		l.closed.Add(2)

		// read input from in Channel and buffer it
		go func() {
			for {
				event, more := <-l.inChannel
				if more {
					l.buffer.AddEvent(event)
				} else {
					l.closed.Done()
					return
				}
			}
		}()

		// read buffer and publish via subscriberMap
		go func() {
			for l.active {
				l.subscriberMap.notify(l.buffer.GetAndRemoveNextEvent())
			}
			l.closed.Done()
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
		return StreamInactive()
	}
	// Publish event...
	l.inChannel <- event
	return nil
}

func (l *LocalAsyncStream[T]) subscribe() (*StreamReceiver[T], error) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if l.active {
		rec := NewStreamReceiver[T](l)
		l.subscriberMap[rec.ID] = rec.Notify
		return rec, nil
	}

	return nil, StreamInactive()
}

func (s *LocalSyncStream[T]) Description() StreamDescription {
	return s.description
}

func (s *LocalSyncStream[T]) ID() StreamID {
	return s.description.StreamID()
}

func (s *LocalSyncStream[T]) Publish(e events.Event[T]) error {
	s.subscriberMap.notify(e)
	return nil
}

func (l *LocalAsyncStream[T]) unsubscribe(id StreamReceiverID) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	l.doUnsubscribe(id)
}

func (l *LocalAsyncStream[T]) doUnsubscribe(id StreamReceiverID) {
	if c, ok := l.subscriberMap[id]; ok {
		delete(l.subscriberMap, id)
		close(c)
	}
}

func (s *LocalSyncStream[T]) unsubscribe(id StreamReceiverID) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	if c, ok := s.subscriberMap[id]; ok {
		delete(s.subscriberMap, id)
		close(c)
	}
}

func (s *LocalSyncStream[T]) subscribe() (*StreamReceiver[T], error) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	rec := NewStreamReceiver[T](s)
	s.subscriberMap[rec.ID] = rec.Notify

	return rec, nil
}

func (s *LocalSyncStream[T]) Run() {

}

func (s *LocalSyncStream[T]) TryClose() {

}

func (s *LocalSyncStream[T]) ForceClose() {
	for id := range s.subscriberMap {
		s.unsubscribe(id)
	}
}

func (s *LocalSyncStream[T]) setNotifiers(m notificationMap[T]) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()
	s.subscriberMap = m
}

func (s *LocalSyncStream[T]) notifiers() notificationMap[T] {
	return s.subscriberMap
}

func (l *LocalAsyncStream[T]) setNotifiers(m notificationMap[T]) {
	l.subscriberMap = m
}

func (l *LocalAsyncStream[T]) notifiers() notificationMap[T] {
	return l.subscriberMap
}

var (
	streamInactive = errors.New("stream not active")
)
