package streams

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/events"
)

type StreamReceiverID uuid.UUID

type StreamReceiver struct {
	Description StreamDescription
	ID          StreamReceiverID
	Notify      chan events.Event
}

func NewStreamReceiver(stream Stream) *StreamReceiver {
	rec := &StreamReceiver{
		Description: stream.Description(),
		ID:          StreamReceiverID(uuid.New()),
		Notify:      make(chan events.Event),
	}
	return rec
}

func (r *StreamReceiver) consumeNextEvent() (events.Event, error) {
	if e, more := <-r.Notify; !more {
		return e, errors.New("channel closed, no more events")
	} else {
		return e, nil
	}
}
