package streams

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"go-stream-processing/events"
	"go.uber.org/zap"
	"sync"
)

type PubSub struct {
	streamIndex    map[StreamID]Stream
	nameIndex      map[string]Stream
	mapAccessMutex sync.RWMutex
}

var PubSubSystem *PubSub

var (
	streamNotFoundError   = errors.New("no stream found")
	streamNameExistsError = errors.New("stream name already exists")
	streamIDNilError      = errors.New("stream id nil")
	streamIDNameDivError  = errors.New("stream id and name do not match")
)

func StreamNotFoundError() error {
	return streamNotFoundError
}
func StreamNameExistsError() error {
	return streamNameExistsError
}
func StreamIDNilError() error     { return streamIDNilError }
func StreamIDNameDivError() error { return streamIDNameDivError }

func (r *PubSub) NewOrReplaceStreamD(description StreamDescription) error {
	var stream Stream

	if description.Async {
		stream = NewLocalAsyncStream(description)
	} else {
		stream = NewLocalSyncStream(description)
	}

	return r.NewOrReplaceStream(stream)
}

func (r *PubSub) NewOrReplaceStream(newStream Stream) error {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()

	if err := r.createOrUpdateStreamIndex(newStream); err != nil {
		return err
	} else {
		newStream.Start()
	}

	zap.S().Info("New stream Added", zap.String("module", "stream"), zap.String("id", newStream.ID().String()))
	return nil
}

func (r *PubSub) RemoveStreamD(description StreamDescription) {
	r.RemoveStream(description.StreamID())
}

func (r *PubSub) RemoveStream(streamID StreamID) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()

	if s, ok := r.streamIndex[streamID]; ok {
		s.Stop()

		delete(r.streamIndex, streamID)
		delete(r.nameIndex, s.Name())

		zap.S().Info("Stream Deleted", zap.String("module", "stream"), zap.String("id", streamID.String()))
	}
}

func (r *PubSub) createDefaultStream(id StreamID) (err error) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if _, ok := r.streamIndex[id]; !ok {
		err = r.createOrUpdateStreamIndex(NewLocalAsyncStream(StreamDescription{Name: fmt.Sprintf("%v", id), ID: uuid.UUID(id)}))
	}
	return nil
}

func (r *PubSub) createOrUpdateStreamIndex(newStream Stream) error {

	err := r.validateStream(newStream)
	if err != nil {
		return err
	}

	if s, ok := r.streamIndex[newStream.ID()]; ok {
		newStream.setNotifiers(s.notifiers())
		newStream.setEvents(s.events())
	}

	r.addStream(newStream)

	return nil
}

func (r *PubSub) addStream(newStream Stream) {
	r.streamIndex[newStream.ID()] = newStream
	r.nameIndex[newStream.Name()] = newStream
}

func (r *PubSub) validateStream(newStream Stream) error {

	if newStream.ID().isNil() {
		return StreamIDNilError()
	}

	//if stream is not indexed, name should not be duplicated
	if idx, ok := r.streamIndex[newStream.ID()]; !ok {
		if _, ok := r.nameIndex[newStream.Name()]; ok {
			zap.S().Error("duplicated name of stream, name needs to be unique")
			return StreamNameExistsError()
		}
	} else {
		if !idx.Description().Equal(newStream.Description()) {
			return StreamIDNameDivError()
		}
	}
	return nil
}

func (r *PubSub) SubscribeN(name string) (*StreamReceiver, error) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if stream, ok := r.nameIndex[name]; ok {
		r := stream.Subscribe()
		return r, nil
	} else {
		return nil, StreamNotFoundError()
	}
}

func (r *PubSub) Subscribe(streamID StreamID) (*StreamReceiver, error) {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if stream, ok := r.streamIndex[streamID]; ok {
		r := stream.Subscribe()
		return r, nil
	} else {
		return nil, StreamNotFoundError()
	}
}

func (r *PubSub) Unsubscribe(rec *StreamReceiver) error {
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if s, ok := r.streamIndex[rec.StreamID]; ok {
		s.Unsubscribe(rec.ID)
		return nil
	} else {
		return StreamNotFoundError()
	}
}

func (r *PubSub) PublishN(name string, event events.Event) error {
	if s, ok := r.nameIndex[name]; ok {
		return s.Publish(event)
	}
	return streamNotFoundError
}

func (r *PubSub) Publish(id StreamID, event events.Event) error {
	if s, ok := r.streamIndex[id]; ok {
		return s.Publish(event)
	}
	return streamNotFoundError
}

func (r *PubSub) GetStream(id StreamID) (Stream, error) {
	if s, ok := r.streamIndex[id]; ok {
		return s, nil
	}
	return nil, streamNotFoundError
}

func (r *PubSub) GetStreamN(name string) (Stream, error) {
	if s, ok := r.nameIndex[name]; ok {
		return s, nil
	}
	return nil, streamNotFoundError
}

func (r *PubSub) GetDescription(id StreamID) (StreamDescription, error) {
	if s, ok := r.streamIndex[id]; ok {
		return s.Description(), nil
	}
	return StreamDescription{}, streamNotFoundError
}

func (r *PubSub) GetDescriptionN(name string) (StreamDescription, error) {
	if s, ok := r.nameIndex[name]; ok {
		return s.Description(), nil
	}
	return StreamDescription{}, streamNotFoundError
}

func init() {
	PubSubSystem = &PubSub{
		streamIndex: make(map[StreamID]Stream),
		nameIndex:   make(map[string]Stream),
	}
}
