package pubsub

import (
	"errors"
	"sync"
	"sync/atomic"

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

	migrateStream(StreamDescription)
	streamMetrics() *StreamMetrics
}

type typedStream[T any] interface {
	stream

	SubscribersManagement[T]

	publisherFanIn[T]
	publishers() publisherManager[T]
	removePublisher(publisherID PublisherID)
	newPublisher() (Publisher[T], error)

	lock()
	unlock()
}

type streamCoordinator[T any] interface {
	publish(events.Event[T]) error
	close()
	run()
}

type baseStream[T any] struct {
	description StreamDescription

	publisherArray publisherManager[T]
	subscriberMap  *notificationMap[T]

	metrics *StreamMetrics

	active  bool
	started bool

	streamCoordinator streamCoordinator[T]

	mutex sync.RWMutex
	cond  *sync.Cond
}

func (b *baseStream[T]) run() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if !b.started {
		b.started = true
		b.active = true

		b.subscriberMap.start()

		b.streamCoordinator.run()
		b.cond.Broadcast()
	}
}

func (b *baseStream[T]) tryClose() bool {
	return b.doTryClose(false)
}

func (b *baseStream[T]) forceClose() {
	b.doTryClose(true)
}

func (b *baseStream[T]) doTryClose(force bool) bool {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if force {
		b.subscriberMap.close()
		b.publisherArray.closePublishers()
	}

	if (!b.hasPublishersOrSubscribers() || force) && b.active {
		b.active = false
		b.streamCoordinator.close()
		return true
	}
	return false
}

func (b *baseStream[T]) migrateStream(description StreamDescription) {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	newEngine := newStreamCoordinator(description, b.subscriberMap)

	b.streamMetrics().WaitUntilDrained()

	b.description = description
	b.subscriberMap.description = description.DefaultSubscribers
	b.streamCoordinator.close()
	b.streamCoordinator = newEngine
	b.streamCoordinator.run()
}

func (b *baseStream[T]) publishSource(content T) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for !b.started {
		b.cond.Wait()
	}

	event := events.NewEvent(content)

	b.metrics.incNumEventsIn()
	return b.streamCoordinator.publish(event)
}

func (b *baseStream[T]) publishComplex(event events.Event[T]) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for !b.started {
		b.cond.Wait()
	}

	b.metrics.incNumEventsIn()
	return b.streamCoordinator.publish(event)
}

func (b *baseStream[T]) newPublisher() (Publisher[T], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.publisherArray.newPublisher(b.ID(), b)
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
	subscriberMap subscribers[T]
}

type localAsyncStream[T any] struct {
	streamChan    events.EventChannel[T]
	closed        sync.WaitGroup
	subscriberMap subscribers[T]
}

// newStreamFromDescription creates and returns a typedStream of a given stream type T
func newStreamFromDescription[T any](description StreamDescription) typedStream[T] {
	var (
		streamCoordinator streamCoordinator[T]
		metrics           = newStreamMetrics()
		subscribers       = newNotificationMap[T](description.DefaultSubscribers, metrics)
	)

	streamCoordinator = newStreamCoordinator(description, subscribers)

	stream := &baseStream[T]{
		description:       description,
		subscriberMap:     subscribers,
		publisherArray:    newPublisherManager[T](),
		metrics:           metrics,
		active:            false,
		started:           false,
		streamCoordinator: streamCoordinator,
	}
	stream.cond = sync.NewCond(&stream.mutex)

	return stream
}

func newStreamCoordinator[T any](description StreamDescription, subscribers *notificationMap[T]) (streamEngine streamCoordinator[T]) {
	if description.Asynchronous {
		streamEngine = newLocalAsyncStream[T](subscribers, description)
	} else {
		streamEngine = newLocalSyncStream[T](subscribers)
	}
	return streamEngine
}

// newLocalSyncStream is a local in-memory stream that delivers events synchronously
// aka is created w/o event buffering
func newLocalSyncStream[T any](subscribers subscribers[T]) *localSyncStream[T] {
	l := &localSyncStream[T]{
		subscriberMap: subscribers,
	}
	return l
}

// newLocalAsyncStream is created w/ event buffering
func newLocalAsyncStream[T any](subscribers subscribers[T], description StreamDescription) *localAsyncStream[T] {

	var ch events.EventChannel[T]
	if description.BufferCapacity > 0 {
		ch = make(events.EventChannel[T], description.BufferCapacity)
	} else {
		ch = make(events.EventChannel[T])
	}
	a := &localAsyncStream[T]{
		subscriberMap: subscribers,
		closed:        sync.WaitGroup{},
		streamChan:    ch,
	}
	return a
}

func (s *localSyncStream[T]) close() {
}

func (l *localAsyncStream[T]) close() {
	close(l.streamChan)
	l.closed.Wait()
}

func (l *localAsyncStream[T]) run() {

	// read buffer and publishSource via subscriberMap
	l.closed.Go(func() {
		for e := range l.streamChan {
			if e != nil {
				_ = l.subscriberMap.notify(e)
			}
		}
	})

}

func (s *localSyncStream[T]) run() {
}

func (b *baseStream[T]) subscribers() subscribers[T] {
	return b.subscriberMap
}

func (s *localSyncStream[T]) publish(event events.Event[T]) error {
	return s.subscriberMap.notify(event)
}

func (l *localAsyncStream[T]) publish(event events.Event[T]) error {
	l.streamChan <- event
	return nil
}

func (b *baseStream[T]) removePublisher(publisherID PublisherID) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.publisherArray.removePublisher(publisherID)
}

func (b *baseStream[T]) unsubscribe(id SubscriberID) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.subscriberMap.remove(id)
}

func (b *baseStream[T]) subscribe(callback func(event events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.active || !b.started {
		return b.subscriberMap.newSubscriber(b.ID(), callback, opts...)
	}

	return nil, StreamInactiveError
}

func (b *baseStream[T]) subscribeBatch(callback func(events ...events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.active || !b.started {
		return b.subscriberMap.newBatchSubscriber(b.ID(), callback, opts...)
	}
	return nil, StreamInactiveError
}

func (b *baseStream[T]) clearPublishers() { b.publisherArray.clearPublishers() }

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
