package streams

import (
	"github.com/google/uuid"
	"go-stream-processing/events"
)

type StreamReceiver struct {
	ID     uuid.UUID
	Notify chan events.Event
}
