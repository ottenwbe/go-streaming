package pubsub

import (
	"errors"
	"sync"
	"sync/atomic"

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
	streamMetrics() *StreamMetrics
}

type typedStream[T any] interface {
	stream

	//publish(events.Event[T]) error

	subscribe() (Subscriber[T], error)
	unsubscribe(id SubscriberID)
	newPublisher() (Publisher[T], error)
	removePublisher(id PublisherID)

	subscribers() *notificationMap[T]
	publishers() publisherFanIn[T]
	inputChannel() events.EventChannel[T]
}

type StreamMetrics struct {
	// metrics
	numEventsIn  atomic.Uint64
	numEventsOut atomic.Uint64
	// sync
	waiting atomic.Bool
	mutex   sync.Mutex
	cond    *sync.Cond
}

func (m *StreamMetrics) incNumEventsIn() {
	m.numEventsIn.Add(1)
}

func (m *StreamMetrics) incNumEventsOut() {
	m.numEventsOut.Add(1)
	if m.waiting.Load() {
		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.cond.Broadcast()
	}
}

func (m *StreamMetrics) NumEventsIn() uint64 {
	return m.numEventsIn.Load()
}

func (m *StreamMetrics) NumEventsOut() uint64 {
	return m.numEventsOut.Load()
}

func (m *StreamMetrics) NumInEventsEqualsNumOutEvents() bool {
	return m.NumEventsIn() == m.NumEventsOut()
}

func (m *StreamMetrics) WaitUntilDrained() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.waiting.Store(true)
	defer m.waiting.Store(false)

	for !m.NumInEventsEqualsNumOutEvents() {
		m.cond.Wait()
	}
}

func newStreamMetrics() *StreamMetrics {
	m := &StreamMetrics{}
	m.cond = sync.NewCond(&m.mutex)
	return m
}

type baseStream[T any] struct {
	description StreamDescription

	publisherMap  publisherFanIn[T]
	subscriberMap *notificationMap[T]

	metrics *StreamMetrics

	notifyMutex sync.Mutex
}

type localSyncStream[T any] struct {
	baseStream[T]
	channel events.EventChannel[T]
}

func (s *localSyncStream[T]) streamMetrics() *StreamMetrics {
	return s.metrics
}

type localAsyncStream[T any] struct {
	baseStream[T]

	inChannel  events.EventChannel[T]
	outChannel events.EventChannel[T]
	buffer     buffer.Buffer[T]

	active bool
	closed sync.WaitGroup
}

func (l *localAsyncStream[T]) streamMetrics() *StreamMetrics {
	return l.metrics
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
	metrics := newStreamMetrics()
	l := &localSyncStream[T]{
		baseStream: baseStream[T]{
			description:   description,
			subscriberMap: newNotificationMap(description, c, metrics),
			publisherMap:  newPublisherSync(description, c, metrics),
			metrics:       metrics,
		},
		channel: c,
	}
	return l
}

// newLocalAsyncStream is created w/ event buffering
func newLocalAsyncStream[T any](description StreamDescription) *localAsyncStream[T] {
	cIn := make(chan events.Event[T])
	cOut := make(chan events.Event[T])
	metrics := newStreamMetrics()

	var buf buffer.Buffer[T]
	// TODO: check if we can use the channel's buffer capacity instead
	if description.BufferCapacity > 0 {
		buf = buffer.NewLimitedSimpleAsyncBuffer[T](description.BufferCapacity)
	} else {
		buf = buffer.NewSimpleAsyncBuffer[T]()
	}
	a := &localAsyncStream[T]{
		baseStream: baseStream[T]{
			description:   description,
			subscriberMap: newNotificationMap(description, cOut, metrics),
			publisherMap:  newPublisherSync(description, cIn, metrics),
			metrics:       metrics,
		},
		active:     false,
		buffer:     buf,
		inChannel:  cIn,
		outChannel: cOut,
	}
	return a
}

func (s *localSyncStream[T]) inputChannel() events.EventChannel[T] {
	return s.channel
}

func (s *localSyncStream[T]) hasPublishersOrSubscribers() bool {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	return s.subscriberMap.len() > 0 || s.publisherMap.len() > 0
}

func (l *localAsyncStream[T]) hasPublishersOrSubscribers() bool {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	return l.subscriberMap.len() > 0 || l.publisherMap.len() > 0
}

func (l *localAsyncStream[T]) forceClose() {
	l.doTryClose(true)
}

func (l *localAsyncStream[T]) tryClose() bool {
	return l.doTryClose(false)
}

func (l *localAsyncStream[T]) doTryClose(force bool) bool {

	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if force {
		l.subscriberMap.close()
		l.publisherMap.close()
	}

	if (l.subscriberMap.len() == 0 || force) && l.active {
		l.active = false
		close(l.inChannel)
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
			defer close(l.outChannel)
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

func (b *baseStream[T]) Description() StreamDescription {
	return b.description
}

func (l *localAsyncStream[T]) ID() StreamID {
	return l.description.StreamID()
}

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

func (l *localAsyncStream[T]) subscribe() (Subscriber[T], error) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if l.active {
		rec := l.subscriberMap.newSubscriber(l.ID())
		return rec, nil
	}

	return nil, StreamInactiveError
}

func (s *localSyncStream[T]) ID() StreamID {
	return s.description.StreamID()
}

func (l *localAsyncStream[T]) unsubscribe(id SubscriberID) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	l.subscriberMap.remove(id)
}

func (s *localSyncStream[T]) unsubscribe(id SubscriberID) {
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

func (s *localSyncStream[T]) subscribe() (Subscriber[T], error) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	rec := s.subscriberMap.newSubscriber(s.ID())

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
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

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
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	s.subscriberMap = m
}

func (l *localAsyncStream[T]) setNotifiers(m *notificationMap[T]) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	l.subscriberMap = m
}

func (s *localSyncStream[T]) copyFrom(stream stream) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	if oldStream, ok := stream.(typedStream[T]); ok {
		s.publisherMap.copyFrom(oldStream.publishers())

		// active waiting until the stream is drained
		oldStream.streamMetrics().WaitUntilDrained()

		s.subscriberMap.copyFrom(oldStream.subscribers())
	}
}

func (l *localAsyncStream[T]) copyFrom(stream stream) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if oldStream, ok := stream.(typedStream[T]); ok {
		l.publisherMap = oldStream.publishers()

		// active waiting until the stream is drained
		oldStream.streamMetrics().WaitUntilDrained()

		l.subscriberMap.copyFrom(oldStream.subscribers())
	}
}

func (s *localSyncStream[T]) publishers() publisherFanIn[T] {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	return s.publisherMap
}

func (l *localAsyncStream[T]) publishers() publisherFanIn[T] {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	return l.publisherMap
}

func (s *localSyncStream[T]) subscribers() *notificationMap[T] {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	return s.subscriberMap
}

func (l *localAsyncStream[T]) subscribers() *notificationMap[T] {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	return l.subscriberMap
}

func (l *localAsyncStream[T]) inputChannel() events.EventChannel[T] {
	return l.inChannel
}
