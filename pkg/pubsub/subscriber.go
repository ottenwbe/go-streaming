package pubsub

import (
	"sync"

	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/selection"

	"github.com/google/uuid"
)

// SubscriberID uniquely identifies a stream receiver.
type SubscriberID uuid.UUID

// String returns the string representation of the SubscriberID.
func (i SubscriberID) String() string {
	return uuid.UUID(i).String()
}

// SubscriptionOption allows to configure the subscription
type SubscriptionOption[T any] func(*subscriptionOptions[T])

type subscriptionOptions[T any] struct {
	policy selection.Policy[T]
}

// WithSelectionPolicy allows to provide a selection policy for the subscriber
func WithSelectionPolicy[T any](p selection.Policy[T]) SubscriptionOption[T] {
	return func(o *subscriptionOptions[T]) {
		o.policy = p
	}
}

// AnySubscriber is the common interface for internal management
type AnySubscriber[T any] interface {
	StreamID() StreamID
	ID() SubscriberID
	doNotify(e events.Event[T])
	close()
}

// Subscriber allows subscribers to get notified about events published in the streams
type Subscriber[T any] interface {
	AnySubscriber[T]
	// Next can be called to wait for and receive the next event.
	// Errors occur when the underlying event channel is closed and no more events can be received.
	Next() (events.Event[T], bool)
}

// BatchSubscriber allows subscribers to get notified about batches of events (e.g. windows)
type BatchSubscriber[T any] interface {
	AnySubscriber[T]
	// Next can be called to wait for and receive the next batch of events.
	// Errors occur when the underlying event channel is closed and no more events can be received.
	Next() ([]events.Event[T], bool)
}

type defaultSubscriber[T any] struct {
	streamID StreamID
	iD       SubscriberID
	notify   events.EventChannel[T]
}

type bufferedSingleSubscriber[T any] struct {
	streamID StreamID
	iD       SubscriberID
	buffer   buffer.Buffer[T]
	active   bool
}

type bufferedBatchSubscriber[T any] struct {
	streamID StreamID
	iD       SubscriberID
	buffer   buffer.Buffer[T]
	active   bool
}

func newDefaultSubscriber[T any](streamID StreamID) Subscriber[T] {
	rec := &defaultSubscriber[T]{
		streamID: streamID,
		iD:       SubscriberID(uuid.New()),
		notify:   make(chan events.Event[T]),
	}
	return rec
}

func newBufferedSingleSubscriber[T any](streamID StreamID, buf buffer.Buffer[T]) Subscriber[T] {
	rec := &bufferedSingleSubscriber[T]{
		streamID: streamID,
		iD:       SubscriberID(uuid.New()),
		buffer:   buf,
		active:   true,
	}
	return rec
}

func newBufferedBatchSubscriber[T any](streamID StreamID, buf buffer.Buffer[T]) BatchSubscriber[T] {
	rec := &bufferedBatchSubscriber[T]{
		streamID: streamID,
		iD:       SubscriberID(uuid.New()),
		buffer:   buf,
		active:   true,
	}
	return rec
}

func (r *defaultSubscriber[T]) close() {
	if r.notify != nil {
		close(r.notify)
	}
}

func (r *defaultSubscriber[T]) doNotify(event events.Event[T]) {
	r.notify <- event
}

func (r *defaultSubscriber[T]) StreamID() StreamID {
	return r.streamID
}

func (r *defaultSubscriber[T]) ID() SubscriberID {
	return r.iD
}

func (r *defaultSubscriber[T]) Next() (events.Event[T], bool) {
	var (
		e    events.Event[T]
		more bool
	)
	e, more = <-r.notify
	return e, more
}

func (r *bufferedSingleSubscriber[T]) doNotify(event events.Event[T]) {
	r.buffer.AddEvent(event)
}

func (r *bufferedSingleSubscriber[T]) close() {
	r.active = false
	r.buffer.StopBlocking()
}

func (r *bufferedSingleSubscriber[T]) StreamID() StreamID {
	return r.streamID
}

func (r *bufferedSingleSubscriber[T]) ID() SubscriberID {
	return r.iD
}

func (r *bufferedSingleSubscriber[T]) Next() (events.Event[T], bool) {
	return r.buffer.GetAndRemoveNextEvent(), r.active
}

func (r *bufferedBatchSubscriber[T]) doNotify(event events.Event[T]) {
	r.buffer.AddEvent(event)
}

func (r *bufferedBatchSubscriber[T]) close() {
	r.active = false
	r.buffer.StopBlocking()
}

func (r *bufferedBatchSubscriber[T]) StreamID() StreamID {
	return r.streamID
}

func (r *bufferedBatchSubscriber[T]) ID() SubscriberID {
	return r.iD
}

func (r *bufferedBatchSubscriber[T]) Next() ([]events.Event[T], bool) {
	return r.buffer.GetAndConsumeNextEvents(), r.active
}

type notificationMap[T any] struct {
	description StreamDescription
	channel     events.EventChannel[T]
	receiver    map[SubscriberID]AnySubscriber[T]
	active      bool
	metrics     *StreamMetrics
	mutex       sync.RWMutex
}

func newNotificationMap[T any](description StreamDescription, inChannel events.EventChannel[T], metrics *StreamMetrics) *notificationMap[T] {
	m := &notificationMap[T]{
		description: description,
		channel:     inChannel,
		receiver:    make(map[SubscriberID]AnySubscriber[T]),
		active:      false,
		metrics:     metrics,
	}
	return m
}

func (m *notificationMap[T]) newSubscriber(streamID StreamID, opts ...SubscriptionOption[T]) (Subscriber[T], error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	options := &subscriptionOptions[T]{}
	for _, opt := range opts {
		opt(options)
	}

	if options.policy != nil {
		return nil, SubscriberPolicyError
	}

	var rec Subscriber[T]
	if m.description.AsyncReceiver {
		var buf buffer.Buffer[T]
		if m.description.BufferCapacity > 0 {
			buf = buffer.NewLimitedSimpleAsyncBuffer[T](m.description.BufferCapacity)
		} else {
			buf = buffer.NewSimpleAsyncBuffer[T]()
		}
		rec = newBufferedSingleSubscriber[T](streamID, buf)
	} else {
		rec = newDefaultSubscriber[T](streamID)
	}

	m.receiver[rec.ID()] = rec

	return rec, nil
}

func (m *notificationMap[T]) newBatchSubscriber(streamID StreamID, opts ...SubscriptionOption[T]) (BatchSubscriber[T], error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	options := &subscriptionOptions[T]{}
	for _, opt := range opts {
		opt(options)
	}

	var rec BatchSubscriber[T]
	if options.policy != nil {
		buf := buffer.NewConsumableAsyncBuffer[T](options.policy)
		rec = newBufferedBatchSubscriber[T](streamID, buf)
	} else {
		// Default batch subscriber (no policy, just buffering)
		var buf buffer.Buffer[T]
		if m.description.BufferCapacity > 0 {
			buf = buffer.NewLimitedSimpleAsyncBuffer[T](m.description.BufferCapacity)
		} else {
			buf = buffer.NewSimpleAsyncBuffer[T]()
		}
		rec = newBufferedBatchSubscriber[T](streamID, buf)
	}

	m.receiver[rec.ID()] = rec

	return rec, nil
}

func (m *notificationMap[T]) close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.active = false
	for id, c := range m.receiver {
		delete(m.receiver, id)
		c.close()
	}
	return nil
}

func (m *notificationMap[T]) remove(id SubscriberID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if c, ok := m.receiver[id]; ok {
		delete(m.receiver, id)
		c.close()
	}
}

func (m *notificationMap[T]) doNotify() {
	for e := range m.channel {

		// avoid concurrency issues with unsubscriptions/subscriptions by working on a snapshot
		targets := m.snapshot()
		for _, notifier := range targets {
			// Wrap in a function to recover from panics if a subscriber is closed concurrently.
			func() {
				defer func() { _ = recover() }()
				notifier.doNotify(e)
			}()
		}
		m.metrics.incNumEventsOut()
	}
}

func (m *notificationMap[T]) snapshot() []AnySubscriber[T] {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	copyOfSubscribers := make([]AnySubscriber[T], 0, len(m.receiver))
	for _, notifier := range m.receiver {
		copyOfSubscribers = append(copyOfSubscribers, notifier)
	}
	return copyOfSubscribers
}

func (m *notificationMap[T]) len() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return len(m.receiver)
}

func (m *notificationMap[T]) copyFrom(old *notificationMap[T]) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	old.mutex.Lock()
	defer old.mutex.Unlock()

	for id, sub := range old.receiver {
		m.receiver[id] = sub
	}
	old.receiver = make(map[SubscriberID]AnySubscriber[T])
}

func (m *notificationMap[T]) start() {
	if !m.active {
		m.active = true
		go m.doNotify()
	}
}
