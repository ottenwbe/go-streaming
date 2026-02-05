package pubsub

import (
	"errors"
	"sync"

	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var StreamInactiveError = errors.New("streams: stream not active")

// stream is a generic interface that is common to all streams with different types.
// Actual streams have a type, i.e., basic ones like int or more complex ones.
type stream interface {
	run()
	tryClose() bool
	forceClose()

	hasPublishersOrSubscribers() bool

	ID() StreamID
	Description() StreamDescription

	copyFrom(stream)
}

type typedStream[T any] interface {
	stream

	//publish(events.Event[T]) error

	subscribe() (StreamReceiver[T], error)
	unsubscribe(id StreamReceiverID)
	newPublisher() (Publisher[T], error)
	removePublisher(id PublisherID)

	subscribers() *notificationMap[T]
	publishers() publisherFanIn[T]
	events() buffer.Buffer[T]
	defaultChannel() events.EventChannel[T]
}

type localSyncStream[T any] struct {
	description StreamDescription

	channel events.EventChannel[T]

	publisherMap  publisherFanIn[T]
	subscriberMap *notificationMap[T]
	// streams are only accessible via pub_sub api, so the following is not required
	//notifyMutex   sync.Mutex
}

type localAsyncStream[T any] struct {
	description StreamDescription

	inChannel  events.EventChannel[T]
	outChannel events.EventChannel[T]
	buffer     buffer.Buffer[T]

	publisherMap  publisherFanIn[T]
	subscriberMap *notificationMap[T]
	// streams are only accessible via pub_sub api, so the following is not required
	// notifyMutex   sync.Mutex

	active bool
	closed sync.WaitGroup
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
		subscriberMap: newNotificationMap(description, c),
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
		subscriberMap: newNotificationMap(description, cOut),
		buffer:        buffer.NewSimpleAsyncBuffer[T](),
		inChannel:     cIn,
		outChannel:    cOut,
		publisherMap:  newPublisherSync(description, cIn),
	}
	return a
}

func (s *localSyncStream[T]) defaultChannel() events.EventChannel[T] {
	return s.channel
}

func (s *localSyncStream[T]) hasPublishersOrSubscribers() bool {
	return s.subscriberMap.len() > 0 || s.publisherMap.len() > 0
}

func (l *localAsyncStream[T]) hasPublishersOrSubscribers() bool {
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

func (l *localAsyncStream[T]) tryClose() bool {
	return l.doTryClose(false)
}

func (l *localAsyncStream[T]) doTryClose(force bool) bool {

	if force {
		l.subscriberMap.close()
		l.publisherMap.close()
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

func (l *localAsyncStream[T]) run() {

	l.subscriberMap.start()

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

	l.publisherMap.remove(id)
}

func (l *localAsyncStream[T]) newPublisher() (Publisher[T], error) {

	return l.publisherMap.newPublisher()
}

func (l *localAsyncStream[T]) subscribe() (StreamReceiver[T], error) {

	if l.active {
		rec := l.subscriberMap.newStreamReceiver(l.ID())
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

func (l *localAsyncStream[T]) unsubscribe(id StreamReceiverID) {

	l.subscriberMap.remove(id)
}

func (s *localSyncStream[T]) unsubscribe(id StreamReceiverID) {

	s.subscriberMap.remove(id)
}

func (s *localSyncStream[T]) newPublisher() (Publisher[T], error) {

	return s.publisherMap.newPublisher()
}

func (s *localSyncStream[T]) removePublisher(id PublisherID) {

	s.publisherMap.remove(id)
}

func (s *localSyncStream[T]) subscribe() (StreamReceiver[T], error) {

	rec := s.subscriberMap.newStreamReceiver(s.ID())

	return rec, nil
}

func (s *localSyncStream[T]) run() {
	s.subscriberMap.start()
}

func (s *localSyncStream[T]) tryClose() bool {
	return s.doForceClose(false)
}

func (s *localSyncStream[T]) forceClose() {
	s.doForceClose(true)
}
func (s *localSyncStream[T]) doForceClose(force bool) bool {

	if force {
		s.subscriberMap.close()
		s.publisherMap.close()
	}
	if s.subscriberMap.len() == 0 || force {
		close(s.channel)
		return true
	}
	return false

}

func (s *localSyncStream[T]) setNotifiers(m *notificationMap[T]) {

	s.subscriberMap = m
}

func (l *localAsyncStream[T]) setNotifiers(m *notificationMap[T]) {
	l.subscriberMap = m
}

func (s *localSyncStream[T]) copyFrom(stream stream) {
	if oldStream, ok := stream.(typedStream[T]); ok {
		s.publisherMap.copyFrom(oldStream.publishers())

		//TODO wait for drain

		s.subscriberMap.copyFrom(oldStream.subscribers())
	}
}

func (l *localAsyncStream[T]) copyFrom(stream stream) {
	if oldStream, ok := stream.(typedStream[T]); ok {
		l.publisherMap = oldStream.publishers()

		//TODO wait for drain

		l.subscriberMap.copyFrom(oldStream.subscribers())
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

func (l *localAsyncStream[T]) defaultChannel() events.EventChannel[T] {
	return l.inChannel
}
