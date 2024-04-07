package streams

import (
	"errors"
	"go-stream-processing/internal/events"
	"go.uber.org/zap"
	"sync"
)

type PubSub struct {
	streamIndex    map[StreamID]interface{}
	nameIndex      map[string]interface{}
	mapAccessMutex sync.RWMutex
}

var PubSubSystem *PubSub

var (
	streamTypeMismatchError = errors.New("stream type mismatch")
	streamNotFoundError     = errors.New("no stream found")
	streamNameExistsError   = errors.New("stream name already exists")
	streamIDNilError        = errors.New("stream id nil")
	streamIDNameDivError    = errors.New("stream id and name do not match")
)

func StreamTypeMismatchError() error {
	return streamTypeMismatchError
}
func StreamNotFoundError() error {
	return streamNotFoundError
}
func StreamNameExistsError() error {
	return streamNameExistsError
}
func StreamIDNilError() error     { return streamIDNilError }
func StreamIDNameDivError() error { return streamIDNameDivError }

func NewOrReplaceStreamD[T any](description StreamDescription) error {
	var stream Stream[T]

	if description.Async {
		stream = NewLocalAsyncStream[T](description)
	} else {
		stream = NewLocalSyncStream[T](description)
	}

	return NewOrReplaceStream(stream)
}

func NewOrReplaceStream[T any](newStream Stream[T]) error {
	PubSubSystem.mapAccessMutex.Lock()
	defer PubSubSystem.mapAccessMutex.Unlock()

	if err := createOrUpdateStreamIndex(newStream); err != nil {
		return err
	} else {
		newStream.Start()
	}

	zap.S().Info("new stream added", zap.String("module", "stream"), zap.String("id", newStream.ID().String()))
	return nil
}

func RemoveStreamD[T any](description StreamDescription) {
	RemoveStream[T](description.StreamID())
}

func RemoveStream[T any](streamID StreamID) {

	PubSubSystem.mapAccessMutex.Lock()
	defer PubSubSystem.mapAccessMutex.Unlock()

	if s, ok := PubSubSystem.streamIndex[streamID]; ok {
		s.(Stream[T]).Stop()

		delete(PubSubSystem.streamIndex, streamID)
		delete(PubSubSystem.nameIndex, s.(Stream[T]).Name())

		zap.S().Info("stream removed", zap.String("module", "stream"), zap.String("id", streamID.String()))
	}
}

func createOrUpdateStreamIndex[T any](newStream Stream[T]) error {

	var r = PubSubSystem
	err := validateStream(newStream)
	if err != nil {
		return err
	}

	if s, ok := r.streamIndex[newStream.ID()]; ok {
		newStream.setNotifiers(s.(Stream[T]).notifiers())
		newStream.setEvents(s.(Stream[T]).events())
	}

	addStream(newStream)

	return nil
}

func addStream[T any](newStream Stream[T]) {
	var r = PubSubSystem
	r.streamIndex[newStream.ID()] = newStream
	r.nameIndex[newStream.Name()] = newStream
}

func validateStream[T any](newStream Stream[T]) error {

	if newStream.ID().isNil() {
		return StreamIDNilError()
	}

	//if stream is not indexed, name should not be duplicated
	if idx, ok := PubSubSystem.streamIndex[newStream.ID()]; !ok {
		if _, ok := PubSubSystem.nameIndex[newStream.Name()]; ok {
			zap.S().Error("duplicated name of stream, name needs to be unique")
			return StreamNameExistsError()
		}
	} else {
		if !idx.(Stream[T]).Description().Equal(newStream.Description()) {
			return StreamIDNameDivError()
		}
	}
	return nil
}

func SubscribeN[T any](name string) (*StreamReceiver[T], error) {
	var r = PubSubSystem
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if stream, ok := r.nameIndex[name]; ok {
		r := stream.(Stream[T]).Subscribe()
		return r, nil
	} else {
		return nil, StreamNotFoundError()
	}
}

func Subscribe[T any](streamID StreamID) (*StreamReceiver[T], error) {
	var r = PubSubSystem
	r.mapAccessMutex.Lock()
	defer r.mapAccessMutex.Unlock()
	if stream, ok := r.streamIndex[streamID]; ok {
		r := stream.(Stream[T]).Subscribe()
		return r, nil
	} else {
		return nil, StreamNotFoundError()
	}
}

func Unsubscribe[T any](rec *StreamReceiver[T]) error {
	PubSubSystem.mapAccessMutex.Lock()
	defer PubSubSystem.mapAccessMutex.Unlock()
	if s, ok := PubSubSystem.streamIndex[rec.StreamID]; ok {
		s.(Stream[T]).Unsubscribe(rec.ID)
		return nil
	} else {
		return StreamNotFoundError()
	}
}

func PublishN[T any](name string, event events.Event[T]) error {
	if s, ok := PubSubSystem.nameIndex[name]; ok {
		return s.(Stream[T]).Publish(event)
	}
	return streamNotFoundError
}

func Publish[T any](id StreamID, event events.Event[T]) error {
	if s, ok := PubSubSystem.streamIndex[id]; ok {
		return s.(Stream[T]).Publish(event)
	}
	return streamNotFoundError
}

func GetStream[T any](id StreamID) (Stream[T], error) {
	if s, ok := PubSubSystem.streamIndex[id]; ok {
		return s.(Stream[T]), nil
	}
	return nil, streamNotFoundError
}

func GetStreamN[T any](name string) (Stream[T], error) {
	if s, ok := PubSubSystem.nameIndex[name]; ok {

		switch s.(type) {
		case Stream[T]:
			return s.(Stream[T]), nil
		default:
			return nil, streamTypeMismatchError
		}
	}
	return nil, streamNotFoundError
}

func GetDescription[T any](id StreamID) (StreamDescription, error) {
	var r *PubSub = PubSubSystem
	if s, ok := r.streamIndex[id]; ok {
		return s.(Stream[T]).Description(), nil
	}
	return StreamDescription{}, streamNotFoundError
}

func GetDescriptionN[T any](name string) (StreamDescription, error) {
	var r = PubSubSystem
	if s, ok := r.nameIndex[name]; ok {
		return s.(Stream[T]).Description(), nil
	}
	return StreamDescription{}, streamNotFoundError
}

func init() {
	PubSubSystem = &PubSub{
		streamIndex: make(map[StreamID]interface{}),
		nameIndex:   make(map[string]interface{}),
	}
}
