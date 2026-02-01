package pubsub

import (
	"errors"
	"go-stream-processing/internal/buffer"
	"go-stream-processing/pkg/events"
	"sync"
)

var StreamInactiveError = errors.New("streams: stream not active")

// Stream is a generic interface that is common to all streams with different types.
// Actual streams have a type, i.e., basic ones like int or more complex ones.
type Stream interface {
	Run()
	TryClose() bool
	forceClose()

	HasPublishersOrSubscribers() bool

	ID() StreamID
	Description() StreamDescription

	copyFrom(Stream)
}

type typedStream[T any] interface {
	Stream

	publish(events.Event[T]) error

	subscribe() (StreamReceiver[T], error)
	unsubscribe(id StreamReceiverID)
	newPublisher() (Publisher[T], error)
	removePublisher(id PublisherID)

	subscribers() notificationMap[T]
	publishers() publisherFanIn[T]
	events() buffer.Buffer[T]
}

type localSyncStream[T any] struct {
	description StreamDescription

	publisherMap  publisherFanIn[T]
	subscriberMap notificationMap[T]
	notifyMutex   sync.Mutex
}

type localAsyncStream[T any] struct {
	description StreamDescription

	inChannel events.EventChannel[T]
	buffer    buffer.Buffer[T]

	publisherMap  publisherFanIn[T]
	subscriberMap notificationMap[T]
	notifyMutex   sync.Mutex

	active bool
	closed sync.WaitGroup
}

// NewStream creates and returns a new stream
// TODO unexport function or remove
func NewStream[T any](topic string, async bool, singleFanIn bool) typedStream[T] {
	return NewStreamFromDescription[T](MakeStreamDescription[T](topic, async, singleFanIn))
}

// NewStreamFromDescription creates and returns a typedStream of a given stream type T
// TODO unexport function
func NewStreamFromDescription[T any](description StreamDescription) typedStream[T] {
	var stream typedStream[T]
	if description.Async {
		stream = newLocalAsyncStream[T](description)
	} else {
		stream = newLocalSyncStream[T](description)
	}
	return stream
}

// newLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func newLocalSyncStream[T any](description StreamDescription) *localSyncStream[T] {
	l := &localSyncStream[T]{
		description:   description,
		subscriberMap: make(notificationMap[T]),
	}
	l.publisherMap = newPublisherSync[T](l, description.SingleFanIn)
	return l
}

// newLocalAsyncStream is created w/ event buffering
func newLocalAsyncStream[T any](description StreamDescription) *localAsyncStream[T] {
	a := &localAsyncStream[T]{
		description:   description,
		active:        false,
		subscriberMap: make(notificationMap[T]),
	}
	a.publisherMap = newPublisherSync[T](a, description.SingleFanIn)
	return a
}

func (s *localSyncStream[T]) HasPublishersOrSubscribers() bool {
	return len(s.subscriberMap) > 0 || s.publisherMap.len() > 0
}

func (l *localAsyncStream[T]) HasPublishersOrSubscribers() bool {
	return len(l.subscriberMap) > 0 || l.publisherMap.len() > 0
}

func (s *localSyncStream[T]) events() buffer.Buffer[T] {
	return buffer.NewSimpleAsyncBuffer[T]()
}

func (l *localAsyncStream[T]) events() buffer.Buffer[T] {
	return l.buffer
}

func (l *localAsyncStream[T]) forceClose() {
	l.doTryClose(true)
}

func (l *localAsyncStream[T]) TryClose() bool {
	return l.doTryClose(false)
}

func (l *localAsyncStream[T]) doTryClose(force bool) bool {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if force {
		l.subscriberMap.clear()
		l.publisherMap.clear()
	}

	if (len(l.subscriberMap) == 0 || force) && l.active {
		l.active = false
		close(l.inChannel)
		l.buffer.StopBlocking()

		l.closed.Wait()
		return true
	}
	return false
}

func (l *localAsyncStream[T]) Run() {
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
				l.subscriberMap.doNotify(l.buffer.GetAndRemoveNextEvent())
			}
			l.closed.Done()
		}()

	}
}

func (l *localAsyncStream[T]) Description() StreamDescription {
	return l.description
}

func (l *localAsyncStream[T]) ID() StreamID {
	return l.description.StreamID()
}

func (l *localAsyncStream[T]) publish(event events.Event[T]) error {
	// Handle stream inactive error
	if !l.active {
		return StreamInactiveError
	}
	// Publish event...
	l.inChannel <- event
	return nil
}

func (l *localAsyncStream[T]) removePublisher(id PublisherID) {
	l.notifyMutex.Lock()
	l.notifyMutex.Unlock()

	l.publisherMap.remove(id)
}

func (l *localAsyncStream[T]) newPublisher() (Publisher[T], error) {
	l.notifyMutex.Lock()
	l.notifyMutex.Unlock()

	return l.publisherMap.newPublisher()
}

func (l *localAsyncStream[T]) subscribe() (StreamReceiver[T], error) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if l.active {
		rec := newStreamReceiver[T](l.ID())
		l.subscriberMap[rec.ID()] = rec.Notify()
		return rec, nil
	}

	return nil, StreamInactiveError
}

func (s *localSyncStream[T]) Description() StreamDescription {
	return s.description
}

func (s *localSyncStream[T]) ID() StreamID {
	return s.description.StreamID()
}

func (s *localSyncStream[T]) publish(e events.Event[T]) error {
	s.subscriberMap.doNotify(e)
	return nil
}

func (l *localAsyncStream[T]) unsubscribe(id StreamReceiverID) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	l.subscriberMap.remove(id)
}

func (s *localSyncStream[T]) unsubscribe(id StreamReceiverID) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	s.subscriberMap.remove(id)
}

func (s *localSyncStream[T]) newPublisher() (Publisher[T], error) {
	s.notifyMutex.Lock()
	s.notifyMutex.Unlock()

	return s.publisherMap.newPublisher()
}

func (s *localSyncStream[T]) removePublisher(id PublisherID) {
	s.notifyMutex.Lock()
	s.notifyMutex.Unlock()

	s.publisherMap.remove(id)
}

func (s *localSyncStream[T]) subscribe() (StreamReceiver[T], error) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	rec := newStreamReceiver[T](s.ID())
	s.subscriberMap[rec.ID()] = rec.Notify()

	return rec, nil
}

func (s *localSyncStream[T]) Run() {

}

func (s *localSyncStream[T]) TryClose() bool {
	return true
}

func (s *localSyncStream[T]) forceClose() {
	s.subscriberMap.clear()
	s.publisherMap.clear()
}

func (s *localSyncStream[T]) setNotifiers(m notificationMap[T]) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()
	s.subscriberMap = m
}

func (l *localAsyncStream[T]) setNotifiers(m notificationMap[T]) {
	l.subscriberMap = m
}

func (s *localSyncStream[T]) copyFrom(stream Stream) {
	if oldStream, ok := stream.(typedStream[T]); ok {
		s.subscriberMap = oldStream.subscribers()
		s.publisherMap = oldStream.publishers()
	}
}

func (l *localAsyncStream[T]) copyFrom(stream Stream) {
	if oldStream, ok := stream.(typedStream[T]); ok {
		l.subscriberMap = oldStream.subscribers()
		l.publisherMap = oldStream.publishers()
		l.buffer.AddEvents(oldStream.events().Dump())
	}
}

func (s *localSyncStream[T]) publishers() publisherFanIn[T] {
	return s.publisherMap
}

func (l *localAsyncStream[T]) publishers() publisherFanIn[T] {
	return l.publisherMap
}

func (s *localSyncStream[T]) subscribers() notificationMap[T] {
	return s.subscriberMap
}

func (l *localAsyncStream[T]) subscribers() notificationMap[T] {
	return l.subscriberMap
}
