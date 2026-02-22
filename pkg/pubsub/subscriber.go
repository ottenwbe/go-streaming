package pubsub

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ottenwbe/go-streaming/pkg/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/selection"

	"github.com/google/uuid"
)

var (
	ErrSubscriberPolicy    = errors.New("subscriber: single subscriber cannot have a selection policy")
	ErrBatchSubscriberSync = errors.New("subscriber: batch subscribers need to be async")
	ErrNotification        = errors.New("subscriber: could not notify all subscribers")
)

type subscribers[T any] interface {
	newSubscriber(streamID StreamID, callback func(event events.Event[T]), options ...SubscriberOption) (Subscriber[T], error)
	newBatchSubscriber(streamID StreamID, callback func(events ...events.Event[T]), options ...SubscriberOption) (Subscriber[T], error)
	close() error
	remove(id SubscriberID)
	notify(event events.Event[T]) error
	snapshot() []Subscriber[T]
	copyFrom(old *notificationMap[T])
	start()
}

type SubscribersManagement[T any] interface {
	subscribe(callback func(event events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error)
	subscribeBatch(callback func(events ...events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error)
	unsubscribe(id SubscriberID)
	subscribers() subscribers[T]
}

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
	doNotify(e events.Event[T]) error
	close()
}

type defaultSubscriber[T any] struct {
	streamID StreamID
	iD       SubscriberID
	active   atomic.Bool
	notify   func(event events.Event[T])
}

type bufferedSubscriber[T any] struct {
	streamID    StreamID
	iD          SubscriberID
	buffer      buffer.Buffer[T]
	active      atomic.Bool
	notify      func(event events.Event[T])
	notifyBatch func(events ...events.Event[T])
	wg          sync.WaitGroup
}

func newDefaultSubscriber[T any](streamID StreamID, callback func(event events.Event[T])) Subscriber[T] {
	rec := &defaultSubscriber[T]{
		streamID: streamID,
		iD:       SubscriberID(uuid.New()),
		notify:   callback,
	}
	rec.active.Store(true)
	return rec
}

func newBufferedSubscriber[T any](streamID StreamID, buf buffer.Buffer[T], callback func(event events.Event[T]), callbackBatch func(events ...events.Event[T])) Subscriber[T] {
	rec := &bufferedSubscriber[T]{
		streamID:    streamID,
		iD:          SubscriberID(uuid.New()),
		buffer:      buf,
		notify:      nil,
		notifyBatch: nil,
	}

	rec.active.Store(true)

	if callback != nil {
		rec.notify = callback
		rec.notifyBatch = rec.drainCallbackBatch
		go rec.notifyNext()
	} else if callbackBatch != nil {
		rec.notify = rec.drainCallback
		rec.notifyBatch = callbackBatch
		go rec.notifyNextBatch()
	}

	return rec
}

func (r *defaultSubscriber[T]) close() {
	r.active.Store(false)
}

func (r *defaultSubscriber[T]) doNotify(event events.Event[T]) error {
	defer func() {
		_ = recover()
	}()
	if r.active.Load() {
		r.notify(event)
	}
	return nil
}

func (r *defaultSubscriber[T]) drainCallback(events.Event[T]) {

}

func (r *defaultSubscriber[T]) StreamID() StreamID {
	return r.streamID
}

func (r *defaultSubscriber[T]) ID() SubscriberID {
	return r.iD
}

func (r *bufferedSubscriber[T]) doNotify(event events.Event[T]) error {
	return r.buffer.AddEvent(event)
}

func (r *bufferedSubscriber[T]) drainCallbackBatch(...events.Event[T]) {

}

func (r *bufferedSubscriber[T]) drainCallback(events.Event[T]) {

}

func (r *bufferedSubscriber[T]) close() {
	r.active.Store(false)
	r.buffer.StopBlocking()
	r.wg.Wait()
}

func (r *bufferedSubscriber[T]) StreamID() StreamID {
	return r.streamID
}

func (r *bufferedSubscriber[T]) ID() SubscriberID {
	return r.iD
}

func (r *bufferedSubscriber[T]) notifyNextBatch() {
	r.wg.Add(1)
	defer r.wg.Done()
	for r.active.Load() {
		e := r.buffer.GetAndConsumeNextEvents()
		if e != nil {
			r.notifyBatch(e...)
		}
	}
}

func (r *bufferedSubscriber[T]) notifyNext() {
	r.wg.Add(1)
	defer r.wg.Done()
	for r.active.Load() {
		e := r.buffer.GetAndRemoveNextEvent()
		if e != nil {
			r.notify(e)
		} else {
			// Buffer stopped
			return
		}
	}
}

type notificationMap[T any] struct {
	description SubscriberDescription
	receiver    map[SubscriberID]Subscriber[T]
	active      bool
	metrics     *StreamMetrics
	mutex       sync.RWMutex
}

func newNotificationMap[T any](description SubscriberDescription, metrics *StreamMetrics) *notificationMap[T] {
	m := &notificationMap[T]{
		description: description,
		receiver:    make(map[SubscriberID]Subscriber[T]),
		active:      false,
		metrics:     metrics,
	}
	return m
}

func (m *notificationMap[T]) newSubscriber(streamID StreamID, callback func(event events.Event[T]), options ...SubscriberOption) (Subscriber[T], error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var description = m.description
	if len(options) > 0 {
		EnrichSubscriberDescription(&description, options...)

		// validate description
		if description.BufferPolicySelection.Active {
			return nil, ErrSubscriberPolicy
		}
	}

	// subscribe
	var rec Subscriber[T]
	if !description.Synchronous {
		rec = newBufferedSubscriber[T](streamID, newBufferForSubscriber[T](description, nil), callback, nil)
	} else {
		rec = newDefaultSubscriber[T](streamID, callback)
	}

	m.receiver[rec.ID()] = rec

	return rec, nil
}

func (m *notificationMap[T]) newBatchSubscriber(streamID StreamID, callback func(events ...events.Event[T]), options ...SubscriberOption) (Subscriber[T], error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var description = m.description
	if len(options) > 0 {
		EnrichSubscriberDescription(&description, options...)

		// validate description
		if description.Synchronous {
			return nil, ErrBatchSubscriberSync
		}
	}

	var rec Subscriber[T]
	var buf buffer.Buffer[T]

	if description.BufferPolicySelection.Active {
		p, err := selection.NewPolicyFromDescription[T](description.BufferPolicySelection)
		if err != nil {
			return nil, err
		}
		buf = newBufferForSubscriber[T](description, p)
	} else {
		// Default batch subscriber (no policy, just buffering)
		buf = newBufferForSubscriber[T](description, nil)
	}
	rec = newBufferedSubscriber[T](streamID, buf, nil, callback)

	m.receiver[rec.ID()] = rec

	return rec, nil
}

func newBufferForSubscriber[T any](description SubscriberDescription, p selection.Policy[T]) buffer.Buffer[T] {
	if p != nil { // policy based
		if description.BufferCapacity > 0 {
			return buffer.NewLimitedConsumableAsyncBuffer[T](p, description.BufferCapacity)
		}
		return buffer.NewConsumableAsyncBuffer[T](p)
	}
	// simple buffer
	if description.BufferCapacity > 0 {
		return buffer.NewLimitedSimpleAsyncBuffer[T](description.BufferCapacity)
	}
	return buffer.NewSimpleAsyncBuffer[T]()
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

func (m *notificationMap[T]) notify(event events.Event[T]) error {

	// Prevent blocking Subscribe/Unsubscribe if a subscriber is slow.
	snapshot := m.snapshot()

	var err error
	for _, notifier := range snapshot {
		if event != nil {
			if errNotify := notifier.doNotify(event); errNotify != nil {
				err = ErrNotification
			}
		}
	}
	m.metrics.incNumEventsOut()

	return err
}

//
//func (m *notificationMap[T]) doNotify() {
//
//	for e := range m.channel {
//
//		// avoid concurrency issues with unsubscriptions/subscriptions by working on a snapshot
//		targets := m.snapshot()
//		for _, notifier := range targets {
//			// Wrap in a function to recover from panics if a subscriber is closed concurrently.
//			func() {
//				defer func() { _ = recover() }()
//				notifier.doNotify(e)
//			}()
//		}
//		m.metrics.incNumEventsOut()
//	}
//}

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
	}
}
