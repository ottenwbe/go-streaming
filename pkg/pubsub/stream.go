package pubsub

import (
	"errors"
	"slices"
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

	migrateStream(stream)
	streamMetrics() *StreamMetrics
}

type typedStream[T any] interface {
	stream

	subscribe(opts ...SubscriberOption) (Subscriber[T], error)
	subscribeBatch(opts ...SubscriberOption) (BatchSubscriber[T], error)
	unsubscribe(id SubscriberID)
	subscribers() *notificationMap[T]

	publisherFanIn[T]
}

type baseStream[T any] struct {
	description StreamDescription

	publisherArray publisherArr[T]
	subscriberMap  *notificationMap[T]

	metrics *StreamMetrics

	publisherMutex  sync.RWMutex
	subscriberMutex sync.RWMutex
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

type localSyncStream[T any] struct {
	baseStream[T]
	active  bool
	started bool
}

type localAsyncStream[T any] struct {
	baseStream[T]

	buffer buffer.Buffer[T]

	active  bool
	started bool
	closed  sync.WaitGroup
}

// newStreamFromDescription creates and returns a typedStream of a given stream type T
func newStreamFromDescription[T any](description StreamDescription) typedStream[T] {
	var stream typedStream[T]
	if description.Asynchronous {
		stream = newLocalAsyncStream[T](description)
	} else {
		stream = newLocalSyncStream[T](description)
	}
	return stream
}

// newLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func newLocalSyncStream[T any](description StreamDescription) *localSyncStream[T] {
	metrics := newStreamMetrics()
	l := &localSyncStream[T]{
		baseStream: baseStream[T]{
			description:    description,
			subscriberMap:  newNotificationMap[T](description.DefaultSubscribers, metrics),
			publisherArray: newPublisherFanIn[T](),
			metrics:        metrics,
		},
		active:  false,
		started: false,
	}
	return l
}

// newLocalAsyncStream is created w/ event buffering
func newLocalAsyncStream[T any](description StreamDescription) *localAsyncStream[T] {
	metrics := newStreamMetrics()

	var buf buffer.Buffer[T]
	if description.BufferCapacity > 0 {
		buf = buffer.NewLimitedSimpleAsyncBuffer[T](description.BufferCapacity)
	} else {
		buf = buffer.NewSimpleAsyncBuffer[T]()
	}
	a := &localAsyncStream[T]{
		baseStream: baseStream[T]{
			description:    description,
			subscriberMap:  newNotificationMap[T](description.DefaultSubscribers, metrics),
			publisherArray: newPublisherFanIn[T](),
			metrics:        metrics,
		},
		active:  false,
		started: false,
		buffer:  buf,
	}
	return a
}

func (l *localAsyncStream[T]) forceClose() {
	l.doTryClose(true)
}

func (l *localAsyncStream[T]) tryClose() bool {
	return l.doTryClose(false)
}

func (s *localSyncStream[T]) doTryClose(force bool) bool {
	s.subscriberMutex.Lock()
	defer s.subscriberMutex.Unlock()
	s.publisherMutex.Lock()
	defer s.publisherMutex.Unlock()

	if force {
		s.subscriberMap.close()
		s.closePublishers()
	}

	if (!s.hasPublishersOrSubscribers() || force) && s.active {
		s.active = false
		return true
	}
	return false
}

func (l *localAsyncStream[T]) doTryClose(force bool) bool {

	l.subscriberMutex.Lock()
	defer l.subscriberMutex.Unlock()
	l.publisherMutex.Lock()
	defer l.publisherMutex.Unlock()

	if force {
		l.subscriberMap.close()
		//formerly l.publisherArray.close()
		l.closePublishers()
	}

	if (!l.hasPublishersOrSubscribers() || force) && l.active {
		l.active = false
		l.buffer.StopBlocking()

		l.closed.Wait()
		return true
	}
	return false
}

func (l *localAsyncStream[T]) run() {

	l.subscriberMutex.Lock()
	defer l.subscriberMutex.Unlock()
	l.publisherMutex.Lock()
	defer l.publisherMutex.Unlock()

	l.subscriberMap.start()

	if !l.active {
		l.started = true
		l.active = true
		l.closed = sync.WaitGroup{}
		l.closed.Add(1)

		// read buffer and publish via subscriberMap
		go func() {
			for l.active {
				e := l.buffer.GetAndRemoveNextEvent()
				if e != nil {
					_ = l.subscriberMap.notify(e)
				}
			}
			l.closed.Done()
		}()

	}
}

func (l *localAsyncStream[T]) subscribe(opts ...SubscriberOption) (Subscriber[T], error) {
	l.subscriberMutex.Lock()
	defer l.subscriberMutex.Unlock()

	if l.active || !l.started {
		return l.subscriberMap.newSubscriber(l.ID(), opts...)
	}

	return nil, StreamInactiveError
}

func (l *localAsyncStream[T]) subscribeBatch(opts ...SubscriberOption) (BatchSubscriber[T], error) {
	l.subscriberMutex.Lock()
	defer l.subscriberMutex.Unlock()

	if l.active || !l.started {
		return l.subscriberMap.newBatchSubscriber(l.ID(), opts...)
	}
	return nil, StreamInactiveError
}

func (b *baseStream[T]) unsubscribe(id SubscriberID) {
	b.subscriberMutex.Lock()
	defer b.subscriberMutex.Unlock()

	b.subscriberMap.remove(id)
}

func (s *localSyncStream[T]) subscribe(opts ...SubscriberOption) (Subscriber[T], error) {
	s.subscriberMutex.Lock()
	defer s.subscriberMutex.Unlock()

	if s.active || !s.started {
		return s.subscriberMap.newSubscriber(s.ID(), opts...)
	}

	return nil, StreamInactiveError
}

func (s *localSyncStream[T]) subscribeBatch(opts ...SubscriberOption) (BatchSubscriber[T], error) {
	s.subscriberMutex.Lock()
	defer s.subscriberMutex.Unlock()

	if s.active || !s.started {
		return s.subscriberMap.newBatchSubscriber(s.ID(), opts...)
	}
	return nil, StreamInactiveError
}

func (s *localSyncStream[T]) run() {
	s.subscriberMutex.Lock()
	defer s.subscriberMutex.Unlock()
	s.publisherMutex.Lock()
	defer s.publisherMutex.Unlock()

	s.started = true
	s.active = true
	s.subscriberMap.start()
}

func (s *localSyncStream[T]) tryClose() bool {
	return s.doTryClose(false)
}

func (s *localSyncStream[T]) forceClose() {
	s.doTryClose(true)
}

func (s *localSyncStream[T]) migrateStream(stream stream) {
	s.subscriberMutex.Lock()
	defer s.subscriberMutex.Unlock()
	s.publisherMutex.Lock()
	defer s.publisherMutex.Unlock()

	if oldStream, ok := stream.(typedStream[T]); ok {
		migratePublishers(s, oldStream)

		// waiting until the stream is drained
		oldStream.streamMetrics().WaitUntilDrained()

		s.subscriberMap.copyFrom(oldStream.subscribers())
	}
}

func (l *localAsyncStream[T]) migrateStream(stream stream) {
	l.subscriberMutex.Lock()
	defer l.subscriberMutex.Unlock()
	l.publisherMutex.Lock()
	defer l.publisherMutex.Unlock()

	if oldStream, ok := stream.(typedStream[T]); ok {
		migratePublishers(l, oldStream)

		// waiting until the stream is drained
		oldStream.streamMetrics().WaitUntilDrained()

		l.subscriberMap.copyFrom(oldStream.subscribers())
	}
}

func (b *baseStream[T]) subscribers() *notificationMap[T] {
	b.subscriberMutex.RLock()
	defer b.subscriberMutex.RUnlock()

	return b.subscriberMap
}

func (s *localSyncStream[T]) publish(event events.Event[T]) error {
	s.publisherMutex.RLock()
	defer s.publisherMutex.RUnlock()

	s.metrics.incNumEventsIn()
	return s.subscriberMap.notify(event)
}

func (s *localSyncStream[T]) publishC(content T) error {
	e := events.NewEvent(content)
	return s.publish(e)
}

func (l *localAsyncStream[T]) publish(event events.Event[T]) error {
	l.publisherMutex.RLock()
	defer l.publisherMutex.RUnlock()

	l.metrics.incNumEventsIn()
	return l.buffer.AddEvent(event)
}

func (l *localAsyncStream[T]) publishC(content T) error {
	e := events.NewEvent(content)
	return l.publish(e)
}

func (b *baseStream[T]) closePublishers() {

	// ensure no publisher is dangling
	for _, publisher := range b.publisherArray {
		if publisher != nil {
			publisher.fanIn = emptyPublisherFanIn[T]{}
		}
	}

	b.clearPublishers()
}

func (s *localSyncStream[T]) newPublisher() (Publisher[T], error) {
	s.publisherMutex.Lock()
	defer s.publisherMutex.Unlock()

	publisher := newDefaultPublisher[T](s.ID(), s)
	s.publisherArray = append(s.publisherArray, publisher)

	return publisher, nil

}

func (l *localAsyncStream[T]) newPublisher() (Publisher[T], error) {
	l.publisherMutex.Lock()
	defer l.publisherMutex.Unlock()

	publisher := newDefaultPublisher[T](l.ID(), l)
	l.publisherArray = append(l.publisherArray, publisher)

	return publisher, nil

}

func (b *baseStream[T]) removePublisher(publisherID PublisherID) {
	b.publisherMutex.Lock()
	defer b.publisherMutex.Unlock()

	if idx := slices.IndexFunc(b.publisherArray, func(publisher *defaultPublisher[T]) bool { return publisherID == publisher.ID() }); idx != -1 {
		b.publisherArray = append(b.publisherArray[:idx], b.publisherArray[idx+1:]...)
	}
}

func (b *baseStream[T]) publishers() publisherArr[T] { return b.publisherArray }

func (b *baseStream[T]) clearPublishers() { b.publisherArray = make(publisherArr[T], 0) }

func migratePublishers[T any](new publisherFanIn[T], old publisherFanIn[T]) {
	for _, pub := range old.publishers() {
		pub.fanIn = new
		new.addPublisher(pub)
	}

	old.clearPublishers()
}

func (b *baseStream[T]) streamMetrics() *StreamMetrics {
	return b.metrics
}

func (b *baseStream[T]) ID() StreamID {
	return b.description.ID
}

func (b *baseStream[T]) Description() StreamDescription {
	return b.description
}

func (b *baseStream[T]) addPublisher(pub *defaultPublisher[T]) {
	b.publisherArray = append(b.publisherArray, pub)
}

func (b *baseStream[T]) hasPublishersOrSubscribers() bool {
	return b.subscriberMap.len() > 0 || b.publisherArray.len() > 0
}
