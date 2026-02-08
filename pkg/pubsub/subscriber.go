package pubsub

import (
	"sync"

	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"

	"github.com/google/uuid"
)

// SubscriberID uniquely identifies a stream receiver.
type SubscriberID uuid.UUID

// String returns the string representation of the SubscriberID.
func (i SubscriberID) String() string {
	return uuid.UUID(i).String()
}

// Subscriber allows subscribers to get notified about events published in the streams
type Subscriber[T any] interface {
	StreamID() StreamID
	ID() SubscriberID
	doNotify(e events.Event[T])
	// Next can be called to wait for and receive the next batch of events.
	// Errors occur when the underlying event channel is closed and no more events can be received.
	Next() ([]events.Event[T], bool)
	close()
}

type defaultSubscriber[T any] struct {
	streamID StreamID
	iD       SubscriberID
	notify   events.EventChannel[T]
}

type bufferedSubscriber[T any] struct {
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

func newBufferedSubscriber[T any](streamID StreamID, buf buffer.Buffer[T]) Subscriber[T] {
	rec := &bufferedSubscriber[T]{
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

func (r *defaultSubscriber[T]) Next() ([]events.Event[T], bool) {
	var (
		e    events.Event[T]
		more bool
	)
	e, more = <-r.notify
	return []events.Event[T]{e}, more
}

func (r *bufferedSubscriber[T]) doNotify(event events.Event[T]) {
	r.buffer.AddEvent(event)
}

func (r *bufferedSubscriber[T]) close() {
	r.active = false
	r.buffer.StopBlocking()
}

func (r *bufferedSubscriber[T]) StreamID() StreamID {
	return r.streamID
}

func (r *bufferedSubscriber[T]) ID() SubscriberID {
	return r.iD
}

func (r *bufferedSubscriber[T]) Next() ([]events.Event[T], bool) {
	return r.buffer.GetAndConsumeNextEvents(), r.active
}

type notificationMap[T any] struct {
	description StreamDescription
	channel     events.EventChannel[T]
	receiver    map[SubscriberID]Subscriber[T]
	active      bool
	metrics     *StreamMetrics
	mutex       sync.RWMutex
}

func newNotificationMap[T any](description StreamDescription, inChannel events.EventChannel[T], metrics *StreamMetrics) *notificationMap[T] {
	m := &notificationMap[T]{
		description: description,
		channel:     inChannel,
		receiver:    make(map[SubscriberID]Subscriber[T]),
		active:      false,
		metrics:     metrics,
	}
	return m
}

func (m *notificationMap[T]) newSubscriber(streamID StreamID, opts ...SubscriptionOption[T]) Subscriber[T] {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	options := &subscriptionOptions[T]{}
	for _, opt := range opts {
		opt(options)
	}

	var rec Subscriber[T]
	if options.policy != nil {
		buf := buffer.NewConsumableAsyncBuffer[T](options.policy)
		rec = newBufferedSubscriber[T](streamID, buf)
	} else if m.description.AsyncReceiver {
		var buf buffer.Buffer[T]
		if m.description.BufferCapacity > 0 {
			buf = buffer.NewLimitedSimpleAsyncBuffer[T](m.description.BufferCapacity)
		} else {
			buf = buffer.NewSimpleAsyncBuffer[T]()
		}
		rec = newBufferedSubscriber[T](streamID, buf)
	} else {
		rec = newDefaultSubscriber[T](streamID)
	}

	m.receiver[rec.ID()] = rec

	return rec
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

func (m *notificationMap[T]) snapshot() []Subscriber[T] {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	copyOfSubscribers := make([]Subscriber[T], 0, len(m.receiver))
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
	old.receiver = make(map[SubscriberID]Subscriber[T])
}

func (m *notificationMap[T]) start() {
	if !m.active {
		m.active = true
		go m.doNotify()
	}
}
