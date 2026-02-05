package pubsub

import (
	"errors"
	"sync"

	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"

	"github.com/google/uuid"
)

// StreamReceiverID uniquely identifies a stream receiver.
type StreamReceiverID uuid.UUID

// String returns the string representation of the StreamReceiverID.
func (i StreamReceiverID) String() string {
	return uuid.UUID(i).String()
}

// StreamReceiver allows subscribers to get notified about events published in the streams
type StreamReceiver[T any] interface {
	StreamID() StreamID
	ID() StreamReceiverID
	// Notify can be called to directly get notified by the underlying event channel
	Notify() events.EventChannel[T]
	doNotify(e events.Event[T])
	// Consume can be called to wait for and receive (asynchronously) an individual event.
	// Errors occur when the underlying event channel is closed and no more events can be received.
	Consume() (events.Event[T], error)
	close()
}

type streamReceiver[T any] struct {
	streamID StreamID
	iD       StreamReceiverID
	notify   events.EventChannel[T]
}

func newDefaultStreamReceiver[T any](streamID StreamID) StreamReceiver[T] {
	rec := &streamReceiver[T]{
		streamID: streamID,
		iD:       StreamReceiverID(uuid.New()),
		notify:   make(chan events.Event[T]),
	}
	return rec
}

type bufferedStreamReceiver[T any] struct {
	streamID StreamID
	iD       StreamReceiverID
	notify   events.EventChannel[T]
	buffer   buffer.Buffer[T]
	active   bool
}

func newBufferedStreamReceiver[T any](streamID StreamID) StreamReceiver[T] {
	rec := &bufferedStreamReceiver[T]{
		streamID: streamID,
		iD:       StreamReceiverID(uuid.New()),
		notify:   make(chan events.Event[T]),
		buffer:   buffer.NewSimpleAsyncBuffer[T](),
		active:   true,
	}
	go func(receiver *bufferedStreamReceiver[T]) {
		for receiver.active {
			e := receiver.buffer.GetAndRemoveNextEvent()
			if e != nil {
				receiver.notify <- e
			}
		}
		close(receiver.notify)
	}(rec)
	return rec
}

func (r *streamReceiver[T]) close() {
	if r.notify != nil {
		close(r.notify)
	}
}

func (r *streamReceiver[T]) doNotify(event events.Event[T]) {
	r.notify <- event
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

func (r *bufferedStreamReceiver[T]) doNotify(event events.Event[T]) {
	r.buffer.AddEvent(event)
}

func (r *bufferedStreamReceiver[T]) close() {
	r.active = false
	r.buffer.StopBlocking()
}

func (r *bufferedStreamReceiver[T]) StreamID() StreamID {
	return r.streamID
}

func (r *bufferedStreamReceiver[T]) ID() StreamReceiverID {
	return r.iD
}

func (r *bufferedStreamReceiver[T]) Notify() events.EventChannel[T] {
	return r.notify
}

func (r *bufferedStreamReceiver[T]) Consume() (events.Event[T], error) {
	if e, more := <-r.notify; !more {
		return e, errors.New("channel closed, no more events")
	} else {
		return e, nil
	}
}

type notificationMap[T any] struct {
	description StreamDescription
	channel     events.EventChannel[T]
	receiver    map[StreamReceiverID]StreamReceiver[T]
	active      bool
}

func newNotificationMap[T any](description StreamDescription, c events.EventChannel[T]) *notificationMap[T] {
	m := &notificationMap[T]{
		description: description,
		channel:     c,
		receiver:    make(map[StreamReceiverID]StreamReceiver[T]),
		active:      false,
	}
	return m
}

func (m notificationMap[T]) newStreamReceiver(streamID StreamID) StreamReceiver[T] {

	var rec StreamReceiver[T]
	if m.description.AsyncReceiver {
		rec = newBufferedStreamReceiver[T](streamID)
	} else {
		rec = newDefaultStreamReceiver[T](streamID)
	}

	m.receiver[rec.ID()] = rec

	return rec
}

func (m notificationMap[T]) close() error {
	m.active = false
	for id := range m.receiver {
		m.remove(id)
	}
	return nil
}

func (m notificationMap[T]) remove(id StreamReceiverID) {
	if c, ok := m.receiver[id]; ok {
		delete(m.receiver, id)
		c.close()
	}
}

func (m notificationMap[T]) doNotify() {
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
	}
}

func (m notificationMap[T]) len() int {
	return len(m.receiver)
}

func (m notificationMap[T]) copyFrom(old *notificationMap[T]) {
	m.receiver = old.receiver
}

func (m notificationMap[T]) start() {
	if !m.active {
		m.active = true
		go m.doNotify()
	}
}
