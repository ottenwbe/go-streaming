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
	publishers() publisherManager[T]
	removePublisher(publisherID PublisherID)
	newPublisher() (Publisher[T], error)
}

type baseStream[T any] struct {
	description StreamDescription

	publisherArray publisherManager[T]
	subscriberMap  *notificationMap[T]

	metrics *StreamMetrics

	active  bool
	started bool

	mutex sync.RWMutex
	cond  *sync.Cond
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
}

type localAsyncStream[T any] struct {
	baseStream[T]

	buffer buffer.Buffer[T]
	closed sync.WaitGroup
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
			publisherArray: newPublisherManager[T](),
			metrics:        metrics,
			active:         false,
			started:        false,
		},
	}
	l.cond = sync.NewCond(&l.mutex)
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
			publisherArray: newPublisherManager[T](),
			metrics:        metrics,
			active:         false,
			started:        false,
		},
		buffer: buf,
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
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if force {
		s.subscriberMap.close()
		s.publisherArray.closePublishers()
	}

	if (!s.hasPublishersOrSubscribers() || force) && s.active {
		s.active = false
		return true
	}
	return false
}

func (l *localAsyncStream[T]) doTryClose(force bool) bool {

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if force {
		l.subscriberMap.close()
		l.publisherArray.closePublishers()
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

	l.mutex.Lock()
	defer l.mutex.Unlock()

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

func (b *baseStream[T]) unsubscribe(id SubscriberID) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.subscriberMap.remove(id)
}

func (b *baseStream[T]) subscribe(opts ...SubscriberOption) (Subscriber[T], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.active || !b.started {
		return b.subscriberMap.newSubscriber(b.ID(), opts...)
	}

	return nil, StreamInactiveError
}

func (b *baseStream[T]) subscribeBatch(opts ...SubscriberOption) (BatchSubscriber[T], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.active || !b.started {
		return b.subscriberMap.newBatchSubscriber(b.ID(), opts...)
	}
	return nil, StreamInactiveError
}

func (s *localSyncStream[T]) run() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.started = true
	s.active = true
	s.subscriberMap.start()
	s.cond.Broadcast()
}

func (s *localSyncStream[T]) tryClose() bool {
	return s.doTryClose(false)
}

func (s *localSyncStream[T]) forceClose() {
	s.doTryClose(true)
}

func (s *localSyncStream[T]) migrateStream(stream stream) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if oldStream, ok := stream.(typedStream[T]); ok {
		oldStream.lock()
		defer oldStream.unlock()

		s.publisherArray.migratePublishers(s, oldStream.publishers())
		// waiting until the stream is drained
		oldStream.streamMetrics().WaitUntilDrained()

		s.subscriberMap.copyFrom(oldStream.subscribers())
	}
}

func (l *localAsyncStream[T]) migrateStream(stream stream) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if oldStream, ok := stream.(typedStream[T]); ok {
		oldStream.lock()
		defer oldStream.unlock()

		l.publisherArray.migratePublishers(l, oldStream.publishers())

		// waiting until the stream is drained
		oldStream.streamMetrics().WaitUntilDrained()

		l.subscriberMap.copyFrom(oldStream.subscribers())
	}
}

func (b *baseStream[T]) subscribers() *notificationMap[T] {
	return b.subscriberMap
}

func (s *localSyncStream[T]) publishC(content T) error {
	e := events.NewEvent(content)
	return s.publish(e)
}

func (s *localSyncStream[T]) publish(event events.Event[T]) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for !s.started {
		s.cond.Wait()
	}

	s.metrics.incNumEventsIn()

	return s.subscriberMap.notify(event)
}

func (l *localAsyncStream[T]) publish(event events.Event[T]) error {

	l.metrics.incNumEventsIn()

	return l.buffer.AddEvent(event)
}

func (l *localAsyncStream[T]) publishC(content T) error {
	e := events.NewEvent(content)
	return l.publish(e)
}

func (s *localSyncStream[T]) newPublisher() (Publisher[T], error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.publisherArray.newPublisher(s.ID(), s)
}

func (l *localAsyncStream[T]) newPublisher() (Publisher[T], error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.publisherArray.newPublisher(l.ID(), l)
}

func (b *baseStream[T]) removePublisher(publisherID PublisherID) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.publisherArray.removePublisher(publisherID)
}

func (b *baseStream[T]) clearPublishers() { b.clearPublishers() }

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
	b.publisherArray.addPublisher(pub)
}

func (b *baseStream[T]) hasPublishersOrSubscribers() bool {
	return b.subscriberMap.len() > 0 || b.publisherArray.len() > 0
}

func (b *baseStream[T]) lock() {
	b.mutex.Lock()
}

func (b *baseStream[T]) unlock() {
	b.mutex.Unlock()
}

func (b *baseStream[T]) publishers() publisherManager[T] {
	return b.publisherArray
}

func (b *baseStream[T]) waitForStart() {
	for !b.active {
		b.cond.Wait()
	}
}
