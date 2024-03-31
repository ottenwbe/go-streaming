package streams

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/events"
)

type StreamReceiverID uuid.UUID

type StreamReceiver[T any] struct {
	StreamID StreamID
	ID       StreamReceiverID
	Notify   events.EventChannel[T]
}

func NewStreamReceiver[T any](stream Stream[T]) *StreamReceiver[T] {
	rec := &StreamReceiver[T]{
		StreamID: stream.ID(),
		ID:       StreamReceiverID(uuid.New()),
		Notify:   make(chan events.Event[T]),
	}
	return rec
}

func (r *StreamReceiver[T]) consumeNextEvent() (events.Event[T], error) {
	if e, more := <-r.Notify; !more {
		return e, errors.New("channel closed, no more events")
	} else {
		return e, nil
	}
}
