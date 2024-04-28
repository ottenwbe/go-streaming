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

// StreamReceiver allows subscribers to get notified about the streams
type StreamReceiver[T any] interface {
	StreamID() StreamID
	ID() StreamReceiverID
	Notify() events.EventChannel[T]
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
	return r.consumeNextEvent()
}

func newStreamReceiver[T any](stream Stream[T]) StreamReceiver[T] {
	rec := &streamReceiver[T]{
		streamID: stream.ID(),
		iD:       StreamReceiverID(uuid.New()),
		notify:   make(chan events.Event[T]),
	}
	return rec
}

func (r *streamReceiver[T]) consumeNextEvent() (events.Event[T], error) {
	if e, more := <-r.Notify(); !more {
		return e, errors.New("channel closed, no more events")
	} else {
		return e, nil
	}
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
		m.notify(e)
	}
}

func (m notificationMap[T]) notify(e events.Event[T]) {
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
