package pubsub

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/internal/events"
	"go.uber.org/zap"
	"sync"
)

var (
	streamIndex    map[StreamID]interface{}
	topicIndex     map[string]interface{}
	mapAccessMutex sync.RWMutex
)

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
func StreamNotFoundError() error { return streamNotFoundError }
func StreamNameExistsError() error {
	return streamNameExistsError
}
func StreamIDNilError() error     { return streamIDNilError }
func StreamIDNameDivError() error { return streamIDNameDivError }

func GetOrCreateStream[T any](eventTopic string, async bool) (Stream[T], error) {
	var (
		err error
	)

	if existingStream, err := getAndConvertStream[T](eventTopic); existingStream != nil {
		return existingStream, nil
	} else if errors.Is(err, StreamNotFoundError()) {
		return AddOrReplaceStreamD[T](NewStreamDescription(eventTopic, uuid.New(), async))
	}

	return nil, err
}

func AddOrReplaceStreamD[T any](description StreamDescription) (Stream[T], error) {
	var (
		stream Stream[T]
		err    error
	)

	if description.Async {
		stream = NewLocalAsyncStream[T](description)
	} else {
		stream = NewLocalSyncStream[T](description)
	}

	err = AddOrReplaceStream(stream)

	return stream, err
}

func AddOrReplaceStream[T any](newStream Stream[T]) error {
	mapAccessMutex.Lock()
	defer mapAccessMutex.Unlock()

	if err := createOrReplaceStreamIndex(newStream); err != nil {
		return err
	}

	zap.S().Info("new stream added", zap.String("module", "stream"), zap.String("id", newStream.ID().String()))
	return nil
}

func RemoveStreamD[T any](description StreamDescription) {
	RemoveStream[T](description.StreamID())
}

func RemoveStream[T any](streamID StreamID) {

	mapAccessMutex.Lock()
	defer mapAccessMutex.Unlock()

	if s, err := getAndConvertStreamByID[T](streamID); err == nil {

		for id, _ := range s.notifiers() {
			s.unsubscribe(id)
		}
		s.TryClose()

		delete(streamIndex, streamID)
		delete(topicIndex, s.(Stream[T]).Name())

		zap.S().Info("stream removed", zap.String("module", "stream"), zap.String("id", streamID.String()))
	}
}

func TryRemoveStreams(streams []StreamControl) {
	mapAccessMutex.Lock()
	defer mapAccessMutex.Unlock()

	for _, stream := range streams {
		delete(streamIndex, stream.ID())
		delete(topicIndex, stream.Name())
	}

}

func createOrReplaceStreamIndex[T any](newStream Stream[T]) error {

	err := validateStream(newStream)
	if err != nil {
		return err
	}

	if s, ok := streamIndex[newStream.ID()]; ok {
		newStream.setNotifiers(s.(Stream[T]).notifiers())
		newStream.setEvents(s.(Stream[T]).events())
	}

	addStream(newStream)

	return nil
}

func addStream[T any](newStream Stream[T]) {
	streamIndex[newStream.ID()] = newStream
	topicIndex[newStream.Name()] = newStream
}

func validateStream[T any](newStream Stream[T]) error {

	if newStream.ID().isNil() {
		return StreamIDNilError()
	}

	//if stream is not indexed, name should not be duplicated
	if idx, idFound := streamIndex[newStream.ID()]; !idFound {
		if _, nameFound := topicIndex[newStream.Name()]; nameFound {
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

func SubscribeToTopic[T any](name string) (*StreamReceiver[T], error) {
	mapAccessMutex.RLock()
	defer mapAccessMutex.RUnlock()

	if stream, err := GetOrCreateStream[T](name, false); err == nil {
		return stream.subscribe(), nil
	} else {
		return nil, err
	}
}

func getAndConvertStream[T any](name string) (Stream[T], error) {
	if stream, ok := topicIndex[name]; ok {
		switch stream.(type) {
		case Stream[T]:
			return stream.(Stream[T]), nil
		default:
			return nil, streamTypeMismatchError
		}
	}
	return nil, streamNotFoundError
}

func getAndConvertStreamByID[T any](id StreamID) (Stream[T], error) {
	if stream, ok := streamIndex[id]; ok {
		switch stream.(type) {
		case Stream[T]:

			return stream.(Stream[T]), nil
		default:
			return nil, streamTypeMismatchError
		}
	}
	return nil, streamNotFoundError

}

func Subscribe[T any](streamID StreamID) (*StreamReceiver[T], error) {
	mapAccessMutex.RLock()
	defer mapAccessMutex.RUnlock()

	if stream, err := getAndConvertStreamByID[T](streamID); err == nil {
		return stream.subscribe(), nil
	} else {
		return nil, err
	}
}

func Unsubscribe[T any](rec *StreamReceiver[T]) {
	mapAccessMutex.RLock()
	defer mapAccessMutex.RUnlock()

	if s, err := getAndConvertStreamByID[T](rec.StreamID); err == nil {
		s.unsubscribe(rec.ID)
	}
}

func PublishN[T any](name string, event events.Event[T]) error {
	if s, err := GetOrCreateStream[T](name, false); err == nil {
		return s.Publish(event)
	} else {
		return err
	}
}

func Publish[T any](id StreamID, event events.Event[T]) error {
	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s.Publish(event)
	} else {
		return err
	}
}

func GetStream[T any](id StreamID) (Stream[T], error) {
	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s, nil
	} else {
		return nil, err
	}
}

func GetStreamN[T any](name string) (Stream[T], error) {
	if s, err := getAndConvertStream[T](name); err == nil {
		return s, nil
	} else {
		return nil, err
	}
}

func GetDescriptionByID[T any](id StreamID) (StreamDescription, error) {
	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s.(Stream[T]).Description(), nil
	} else {
		return StreamDescription{}, streamNotFoundError
	}
}

func GetDescription[T any](name string) (StreamDescription, error) {
	if s, err := getAndConvertStream[T](name); err == nil {
		return s.(Stream[T]).Description(), nil
	} else {
		return StreamDescription{}, err
	}
}

func init() {

	streamIndex = make(map[StreamID]interface{})
	topicIndex = make(map[string]interface{})

}
