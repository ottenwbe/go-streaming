package streams

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/events"
	"sync"
)

type PubSub struct {
	streams map[uuid.UUID]Stream
	mutex   sync.Mutex
}

var PubSubSystem *PubSub

var StreamNotFoundError = errors.New("no stream found")

func (r *PubSub) NewOrReplaceStream(streamID uuid.UUID, newStream Stream) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if s, ok := r.streams[streamID]; ok {
		newStream.setNotifiers(s.notifiers())
	}
	r.streams[streamID] = newStream
}

func (r *PubSub) Subscribe(streamID uuid.UUID, rec StreamReceiver) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if stream, ok := r.streams[streamID]; ok {
		stream.Subscribe(rec)
	} else {
		return StreamNotFoundError
	}
	return nil
}

func (r *PubSub) Unsubscribe(id uuid.UUID) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, ok := r.streams[id]; ok {
		delete(r.streams, id)
		return nil
	} else {
		return StreamNotFoundError
	}
}

func (r *PubSub) Publish(id uuid.UUID, event events.Event) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if res, ok := r.streams[id]; ok {
		res.Publish(event)
		return nil
	} else {
		return StreamNotFoundError
	}
}

func init() {
	PubSubSystem = &PubSub{
		streams: make(map[uuid.UUID]Stream),
	}
}
