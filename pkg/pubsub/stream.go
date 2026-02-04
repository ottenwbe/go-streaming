package pubsub

import (
	"errors"
	"sync"

	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"
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

	//publish(events.Event[T]) error

	subscribe() (StreamReceiver[T], error)
	unsubscribe(id StreamReceiverID)
	newPublisher() (Publisher[T], error)
	removePublisher(id PublisherID)

	subscribers() *notificationMap[T]
	publishers() publisherFanIn[T]
	events() buffer.Buffer[T]
}

type localSyncStream[T any] struct {
	description StreamDescription

	channel events.EventChannel[T]

	publisherMap  publisherFanIn[T]
	subscriberMap *notificationMap[T]
	notifyMutex   sync.Mutex
}

type localAsyncStream[T any] struct {
	description StreamDescription

	inChannel  events.EventChannel[T]
	outChannel events.EventChannel[T]
	buffer     buffer.Buffer[T]

	publisherMap  publisherFanIn[T]
	subscriberMap *notificationMap[T]
	notifyMutex   sync.Mutex

	active bool
	closed sync.WaitGroup
}

// newStream creates and returns a new stream
func newStream[T any](topic string, async bool, singleFanIn bool) typedStream[T] {
	return newStreamFromDescription[T](MakeStreamDescription[T](topic, WithAsyncStream(async), WithSingleFanIn(singleFanIn)))
}

// newStreamFromDescription creates and returns a typedStream of a given stream type T
func newStreamFromDescription[T any](description StreamDescription) typedStream[T] {
	var stream typedStream[T]
	if description.AsyncStream {
		stream = newLocalAsyncStream[T](description)
	} else {
		stream = newLocalSyncStream[T](description)
	}
	return stream
}

// newLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func newLocalSyncStream[T any](description StreamDescription) *localSyncStream[T] {
	c := make(chan events.Event[T])
	l := &localSyncStream[T]{
		description:   description,
		subscriberMap: newNotificationMap[T](c),
		channel:       c,
		publisherMap:  newPublisherSync(description, c),
	}
	return l
}

// newLocalAsyncStream is created w/ event buffering
func newLocalAsyncStream[T any](description StreamDescription) *localAsyncStream[T] {
	cIn := make(chan events.Event[T])
	cOut := make(chan events.Event[T])
	a := &localAsyncStream[T]{
		description:   description,
		active:        false,
		subscriberMap: newNotificationMap[T](cOut),
		buffer:        buffer.NewSimpleAsyncBuffer[T](),
		inChannel:     cIn,
		outChannel:    cOut,
		publisherMap:  newPublisherSync(description, cIn),
	}
	return a
}

func (s *localSyncStream[T]) HasPublishersOrSubscribers() bool {
	return s.subscriberMap.len() > 0 || s.publisherMap.len() > 0
}

func (l *localAsyncStream[T]) HasPublishersOrSubscribers() bool {
	return l.subscriberMap.len() > 0 || l.publisherMap.len() > 0
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
		l.subscriberMap.Close()
		l.publisherMap.Close()
	}

	if (l.subscriberMap.len() == 0 || force) && l.active {
		l.active = false
		close(l.inChannel)
		close(l.outChannel)
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
				e := l.buffer.GetAndRemoveNextEvent()
				if e != nil {
					l.outChannel <- e
				}
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

//func (l *localAsyncStream[T]) publish(event events.Event[T]) error {
//	// Handle stream inactive error
//	if !l.active {
//		return StreamInactiveError
//	}
//	// Publish event...
//	l.channel <- event
//	return nil
//}

func (l *localAsyncStream[T]) removePublisher(id PublisherID) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	l.publisherMap.remove(id)
}

func (l *localAsyncStream[T]) newPublisher() (Publisher[T], error) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	return l.publisherMap.newPublisher()
}

func (l *localAsyncStream[T]) subscribe() (StreamReceiver[T], error) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if l.active {
		rec := l.subscriberMap.newStreamReceiver(l.ID(), false)
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

//func (s *localSyncStream[T]) publish(e events.Event[T]) error {
//	s.subscriberMap.doNotify(e)
//	return nil
//}

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
	defer s.notifyMutex.Unlock()

	return s.publisherMap.newPublisher()
}

func (s *localSyncStream[T]) removePublisher(id PublisherID) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	s.publisherMap.remove(id)
}

func (s *localSyncStream[T]) subscribe() (StreamReceiver[T], error) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	rec := s.subscriberMap.newStreamReceiver(s.ID(), false)

	return rec, nil
}

func (s *localSyncStream[T]) Run() {

}

func (s *localSyncStream[T]) TryClose() bool {
	return s.doForceClose(false)
}

func (s *localSyncStream[T]) forceClose() {
	s.doForceClose(true)
}
func (s *localSyncStream[T]) doForceClose(force bool) bool {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	if force {
		s.subscriberMap.Close()
		s.publisherMap.Close()
	}
	if s.subscriberMap.len() == 0 || force {
		close(s.channel)
		return true
	}
	return false

}

func (s *localSyncStream[T]) setNotifiers(m *notificationMap[T]) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()
	s.subscriberMap = m
}

func (l *localAsyncStream[T]) setNotifiers(m *notificationMap[T]) {
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

func (s *localSyncStream[T]) subscribers() *notificationMap[T] {
	return s.subscriberMap
}

func (l *localAsyncStream[T]) subscribers() *notificationMap[T] {
	return l.subscriberMap
}
