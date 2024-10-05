package pubsub

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/pkg/events"
)

type StreamReceiverID uuid.UUID

func (i StreamReceiverID) String() string {
	return uuid.UUID(i).String()
}

// StreamReceiver allows subscribers to get notified about events published in the streams
type StreamReceiver[T any] interface {
	StreamID() StreamID
	ID() StreamReceiverID
	// Notify can be called to directly get notified by the underlying event channel
	Notify() events.EventChannel[T]
	// Consume can be called to wait for and receive (asynchronously) an individual event.
	// Errors occur when the underlying event channel is closed and no more events can be received.
	Consume() (events.Event[T], error)
}

type streamReceiver[T any] struct {
	streamID StreamID
	iD       StreamReceiverID
	notify   events.EventChannel[T]
}

func (r *streamReceiver[T]) StreamID() StreamID {
	return r.streamID
}

func (r *streamReceiver[T]) ID() StreamReceiverID {
	return r.iD
}

func (r *streamReceiver[T]) Notify() events.EventChannel[T] {
	return r.notify
}

func (r *streamReceiver[T]) Consume() (events.Event[T], error) {
	if e, more := <-r.notify; !more {
		return e, errors.New("channel closed, no more events")
	} else {
		return e, nil
	}
}

func newStreamReceiver[T any](stream typedStream[T]) StreamReceiver[T] {
	rec := &streamReceiver[T]{
		streamID: stream.ID(),
		iD:       StreamReceiverID(uuid.New()),
		notify:   make(chan events.Event[T]),
	}
	return rec
}

type notificationMap[T any] map[StreamReceiverID]events.EventChannel[T]

func (m notificationMap[T]) clear() {
	for id := range m {
		m.remove(id)
	}
}

func (m notificationMap[T]) remove(id StreamReceiverID) {
	if c, ok := m[id]; ok {
		delete(m, id)
		close(c)
	}
}
func (m notificationMap[T]) notifyAll(events []events.Event[T]) {
	for _, e := range events {
		m.doNotify(e)
	}
}

func (m notificationMap[T]) doNotify(e events.Event[T]) {
	for _, notifier := range m {
		/*
			The code should never panic here, because notifiers are unsubscribed before stream closes.
			However, if the concept changes, consider to handle the panic here:

			defer func() {
				if r := recover(); r != nil {
					zap.S().Debugf("recovered subscriberMap panic for stream %v", id)
				}
			}()*/
		notifier <- e
	}
}
