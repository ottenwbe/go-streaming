package pubsub

import (
	"errors"
	"go-stream-processing/pkg/events"
	"sync"
)

var (
	streamIndex    map[StreamID]interface{}
	mapAccessMutex sync.RWMutex
)

var (
	streamTypeMismatchError = errors.New("stream type mismatch")
	streamNotFoundError     = errors.New("no stream found")
	streamNameExistsError   = errors.New("stream id already exists")
	streamIDNilError        = errors.New("stream id nil")
)

func StreamTypeMismatchError() error {
	return streamTypeMismatchError
}
func StreamNotFoundError() error { return streamNotFoundError }
func StreamNameExistsError() error {
	return streamNameExistsError
}
func StreamIDNilError() error { return streamIDNilError }

func GetOrCreateStream[T any](eventTopic StreamID, async bool) (Stream[T], error) {
	return GetOrCreateStreamD[T](MakeStreamDescription(eventTopic, async))
}

func GetOrCreateStreamD[T any](d StreamDescription) (Stream[T], error) {
	var (
		err            error
		existingStream Stream[T]
	)

	if existingStream, err = getAndConvertStreamByID[T](d.ID); existingStream != nil {
		return existingStream, nil
	} else if errors.Is(err, StreamNotFoundError()) {
		return AddOrReplaceStreamD[T](d)
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

	return nil
}

func ForceRemoveStreamD[T any](description StreamDescription) {
	ForceRemoveStream[T](description.StreamID())
}

func ForceRemoveStream[T any](streamID StreamID) {

	mapAccessMutex.Lock()
	defer mapAccessMutex.Unlock()

	if s, err := getAndConvertStreamByID[T](streamID); err == nil {
		s.ForceClose()
		delete(streamIndex, streamID)
	}
}

func TryRemoveStreams(streams []StreamControl) {
	mapAccessMutex.Lock()
	defer mapAccessMutex.Unlock()

	for _, stream := range streams {
		if !stream.HasPublishersOrSubscribers() {
			delete(streamIndex, stream.ID())
		}
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
}

func validateStream[T any](newStream Stream[T]) error {

	if newStream.ID().IsNil() {
		return StreamIDNilError()
	}

	//if stream is indexed, name should not be duplicated
	if _, idFound := streamIndex[newStream.ID()]; idFound {
		return StreamNameExistsError()
	}
	return nil
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

func Subscribe[T any](id StreamID) (*StreamReceiver[T], error) {
	mapAccessMutex.RLock()
	defer mapAccessMutex.RUnlock()

	if stream, err := getAndConvertStreamByID[T](id); err == nil {
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

func GetDescription[T any](id StreamID) (StreamDescription, error) {
	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s.(Stream[T]).Description(), nil
	} else {
		return StreamDescription{}, StreamNotFoundError()
	}
}

/*func PublishN[T any](name string, event events.Event[T]) error {
	if s, err := GetOrCreateStream[T](name, false); err == nil {
		return s.Publish(event)
	} else {
		return err
	}
}*/
/*func GetStreamN[T any](name string) (Stream[T], error) {
	if s, err := getAndConvertStream[T](name); err == nil {
		return s, nil
	} else {
		return nil, err
	}
}*/
/*func GetDescription[T any](name string) (StreamDescription, error) {
	if s, err := getAndConvertStream[T](name); err == nil {
		return s.(Stream[T]).Description(), nil
	} else {
		return StreamDescription{}, err
	}
}*/
/*func SubscribeToTopic[T any](name string) (*StreamReceiver[T], error) {
	mapAccessMutex.RLock()
	defer mapAccessMutex.RUnlock()

	if stream, err := GetOrCreateStream[T](name, false); err == nil {
		return stream.subscribe(), nil
	} else {
		return nil, err
	}
}*/

/*func getAndConvertStream[T any](name string) (Stream[T], error) {
	if stream, ok := topicIndex[name]; ok {
		switch stream.(type) {
		case Stream[T]:
			return stream.(Stream[T]), nil
		default:
			return nil, streamTypeMismatchError
		}
	}
	return nil, streamNotFoundError
}*/

func init() {
	streamIndex = make(map[StreamID]interface{})
}
