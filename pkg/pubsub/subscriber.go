package pubsub

import (
	"errors"
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
	// Notify can be called to directly get notified by the underlying event channel
	Notify() events.EventChannel[T]
	doNotify(e events.Event[T])
	// Consume can be called to wait for and receive (asynchronously) an individual event.
	// Errors occur when the underlying event channel is closed and no more events can be received.
	Consume() (events.Event[T], error)
	close()
}

type defaultSubscriber[T any] struct {
	streamID StreamID
	iD       SubscriberID
	notify   events.EventChannel[T]
}

func newDefaultSubscriber[T any](streamID StreamID) Subscriber[T] {
	rec := &defaultSubscriber[T]{
		streamID: streamID,
		iD:       SubscriberID(uuid.New()),
		notify:   make(chan events.Event[T]),
	}
	return rec
}

type bufferedSubscriber[T any] struct {
	streamID StreamID
	iD       SubscriberID
	notify   events.EventChannel[T]
	buffer   buffer.Buffer[T]
	active   bool
}

func newBufferedSubscriber[T any](streamID StreamID) Subscriber[T] {
	rec := &bufferedSubscriber[T]{
		streamID: streamID,
		iD:       SubscriberID(uuid.New()),
		notify:   make(chan events.Event[T]),
		buffer:   buffer.NewSimpleAsyncBuffer[T](),
		active:   true,
	}

	go func(receiver *bufferedSubscriber[T]) {
		defer close(receiver.notify)

		for receiver.active {
			e := receiver.buffer.GetAndRemoveNextEvent()
			if e != nil {
				receiver.notify <- e
			}
		}
	}(rec)
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

func (r *defaultSubscriber[T]) Notify() events.EventChannel[T] {
	return r.notify
}

func (r *defaultSubscriber[T]) Consume() (events.Event[T], error) {
	if e, more := <-r.notify; !more {
		return e, errors.New("channel closed, no more events")
	} else {
		return e, nil
	}
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

func (r *bufferedSubscriber[T]) Notify() events.EventChannel[T] {
	return r.notify
}

func (r *bufferedSubscriber[T]) Consume() (events.Event[T], error) {
	if e, more := <-r.notify; !more {
		return e, errors.New("channel closed, no more events")
	} else {
		return e, nil
	}
}

type notificationMap[T any] struct {
	description StreamDescription
	channel     events.EventChannel[T]
	receiver    map[SubscriberID]Subscriber[T]
	active      bool
	metrics     *StreamMetrics
}

func newNotificationMap[T any](description StreamDescription, c events.EventChannel[T], metrics *StreamMetrics) *notificationMap[T] {
	m := &notificationMap[T]{
		description: description,
		channel:     c,
		receiver:    make(map[SubscriberID]Subscriber[T]),
		active:      false,
		metrics:     metrics,
	}
	return m
}

func (m *notificationMap[T]) newStreamReceiver(streamID StreamID) Subscriber[T] {

	var rec Subscriber[T]
	if m.description.AsyncReceiver {
		rec = newBufferedSubscriber[T](streamID)
	} else {
		rec = newDefaultSubscriber[T](streamID)
	}

	m.receiver[rec.ID()] = rec

	return rec
}

func (m *notificationMap[T]) close() error {
	m.active = false
	for id := range m.receiver {
		m.remove(id)
	}
	return nil
}

func (m *notificationMap[T]) remove(id SubscriberID) {
	if c, ok := m.receiver[id]; ok {
		delete(m.receiver, id)
		c.close()
	}
}

func (m *notificationMap[T]) doNotify() {
	for m.active {
		e := <-m.channel
		if e == nil {
			return
		}
		wg := sync.WaitGroup{}

		for _, notifier := range m.receiver {
			/*
				The code should never panic here, because notifiers are unsubscribed before the stream closes.
				However, if the concept changes, consider to handle the panic here:

				defer func() {
					if r := recover(); r != nil {
						zap.S().Debugf("recovered subscriberMap panic for stream %v", id)
					}
				}()*/
			wg.Go(func() {
				notifier.doNotify(e)
			})
		}
		wg.Wait()
		m.metrics.incNumEventsOut()
	}
}

func (m *notificationMap[T]) len() int {
	return len(m.receiver)
}

func (m *notificationMap[T]) copyFrom(old *notificationMap[T]) {
	m.receiver = old.receiver
	old.receiver = make(map[SubscriberID]Subscriber[T])
}

func (m *notificationMap[T]) start() {
	if !m.active {
		m.active = true
		go m.doNotify()
	}
}
