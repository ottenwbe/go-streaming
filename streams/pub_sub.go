package streams

import (
	"errors"
	"fmt"
	"go-stream-processing/events"
	"go.uber.org/zap"
	"sync"
)

type StreamIndex struct {
	stream Stream
	header *StreamHeader
}

type PubSub struct {
	streamIndex    map[StreamID]StreamIndex
	nameIndex      map[string]*StreamHeader
	mapAccessMutex sync.Mutex
}

var PubSubSystem *PubSub

var (
	streamNotFoundError   = errors.New("no stream found")
	streamNameExistsError = errors.New("stream name already exists")
	streamIDNilError      = errors.New("stream id nil")
)

func StreamNotFoundError() error {
	return streamNotFoundError
}
func StreamNameExistsError() error {
	return streamNameExistsError
}
func StreamIDNilError() error {
	return streamIDNilError
}

func (r *PubSub) NewOrReplaceStream(newStream Stream) error {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()

	if err := r.addStream(newStream); err != nil {
		return err
	}
	zap.S().Info("New stream Added", zap.String("module", "stream"), zap.String("id", newStream.Header().ID.String()))
	return nil
}

func (r *PubSub) RemoveStream(streamID StreamID) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()

	if s, ok := r.streamIndex[streamID]; ok {
		for _, c := range s.stream.notifiers() {
			close(c)
		}
		delete(r.streamIndex, streamID)
	}

	zap.S().Info("Stream Deleted", zap.String("module", "stream"), zap.String("id", streamID.String()))
}

func (r *PubSub) createDefaultStream(id StreamID) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if _, ok := r.streamIndex[id]; !ok {
		r.addStream(NewLocalAsyncStream(fmt.Sprintf("%v", id), id))
	}
}

func (r *PubSub) addStream(newStream Stream) error {

	err := r.validateStream(newStream)
	if err != nil {
		return err
	}

	if s, ok := r.streamIndex[newStream.Header().ID]; ok {
		newStream.setNotifiers(s.stream.notifiers())
		newStream.setEvents(s.stream.events())
	}

	header := newStream.Header()
	r.streamIndex[newStream.Header().ID] = StreamIndex{
		stream: newStream,
		header: &header,
	}
	r.nameIndex[newStream.Header().Name] = &header
}

func (r *PubSub) validateStream(newStream Stream) error {

	if newStream.Header().ID.isNil() {
		return StreamIDNilError()
	}

	//
	if _, ok := r.streamIndex[newStream.Header().ID]; !ok {
		if _, ok := r.nameIndex[newStream.Header().Name]; ok {
			zap.S().Error("duplicated name of stream, name needs to be unique")
			return StreamNameExistsError()
		}
	}
	return nil
}

func (r *PubSub) Subscribe(streamID StreamID) (*StreamReceiver, error) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if stream, ok := r.streamIndex[streamID]; ok {
		r := stream.stream.Subscribe()
		return r, nil
	} else {
		return nil, StreamNotFoundError()
	}

}

func (r *PubSub) Unsubscribe(rec *StreamReceiver) error {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if s, ok := r.streamIndex[rec.StreamID]; ok {
		s.stream.Unsubscribe(rec.ID)
		return nil
	} else {
		return StreamNotFoundError()
	}
}

func (r *PubSub) Publish(id StreamID, event events.Event) error {
	if _, ok := r.streamIndex[id]; !ok {
		r.createDefaultStream(id)
	}
	return r.streamIndex[id].stream.Publish(event)
}

func (r *PubSub) Get(id StreamID) (Stream, error) {
	if s, ok := r.streamIndex[id]; ok {
		return s.stream, nil
	}
	return nil, streamNotFoundError
}

func init() {
	PubSubSystem = &PubSub{
		streamIndex: make(map[StreamID]StreamIndex),
		nameIndex:   make(map[string]*StreamHeader),
	}
}
