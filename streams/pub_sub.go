package streams

import (
	"errors"
	"fmt"
	"go-stream-processing/events"
	"go.uber.org/zap"
	"sync"
)

type PubSub struct {
	streams        map[StreamID]Stream
	mapAccessMutex sync.Mutex
}

var PubSubSystem *PubSub

var streamNotFoundError = errors.New("no stream found")

func StreamNotFoundError() error {
	return streamNotFoundError
}

func (r *PubSub) NewOrReplaceStream(streamID StreamID, newStream Stream) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	r.addStream(streamID, newStream)
	zap.S().Info("New stream Added", zap.String("module", "stream"), zap.String("id", streamID.String()))
}

func (r *PubSub) RemoveStream(streamID StreamID) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()

	if s, ok := r.streams[streamID]; ok {
		for _, c := range s.notifiers() {
			close(c)
		}
		delete(r.streams, streamID)
	}

	zap.S().Info("Stream Deleted", zap.String("module", "stream"), zap.String("id", streamID.String()))
}

func (r *PubSub) createDefaultStream(id StreamID) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if _, ok := r.streams[id]; !ok {
		r.addStream(id, NewLocalAsyncStream(fmt.Sprintf("%v", id), id))
	}
}

func (r *PubSub) addStream(streamID StreamID, newStream Stream) {
	if s, ok := r.streams[streamID]; ok {
		newStream.setNotifiers(s.notifiers())
		newStream.setEvents(s.events())
	}
	r.streams[streamID] = newStream
}

func (r *PubSub) Subscribe(streamID StreamID) (*StreamReceiver, error) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if stream, ok := r.streams[streamID]; ok {
		r := stream.Subscribe()
		return r, nil
	} else {
		return nil, StreamNotFoundError()
	}

}

func (r *PubSub) Unsubscribe(rec *StreamReceiver) error {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if s, ok := r.streams[rec.StreamID]; ok {
		s.Unsubscribe(rec.ID)
		return nil
	} else {
		return StreamNotFoundError()
	}
}

func (r *PubSub) Publish(id StreamID, event events.Event) error {
	if _, ok := r.streams[id]; !ok {
		r.createDefaultStream(id)
	}
	return r.streams[id].Publish(event)
}

func (r *PubSub) Get(id StreamID) (Stream, error) {
	if s, ok := r.streams[id]; ok {
		return s, nil
	}
	return nil, streamNotFoundError
}

func init() {
	PubSubSystem = &PubSub{
		streams: make(map[StreamID]Stream),
	}
}
