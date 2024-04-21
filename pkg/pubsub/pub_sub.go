package pubsub

import (
	"errors"
	"sync"
)

var (
	streamIndex    map[StreamID]StreamControl
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

// GetOrAddStream adds a stream to the pub sub system or returns an existing one
// Note: this should only be called if you are sure that the stream has the right type
func GetOrAddStream(stream StreamControl) (StreamControl, error) {

	err := validateStream(stream)
	if err != nil {
		return stream, err
	}

	if existingStream, ok := streamIndex[stream.ID()]; ok {
		return existingStream, nil
	} else {
		addStream(stream)
		return stream, nil
	}
}

func AddOrReplaceStreamD[T any](description StreamDescription) (Stream[T], error) {
	var (
		stream Stream[T]
		err    error
	)

	stream = NewStreamD[T](description)
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

func ForceRemoveStreamD(description StreamDescription) {
	ForceRemoveStream(description.StreamID())
}

func ForceRemoveStream(streamID StreamID) {
	mapAccessMutex.Lock()
	defer mapAccessMutex.Unlock()

	if s, ok := streamIndex[streamID]; ok {
		s.ForceClose()
		delete(streamIndex, streamID)
	}
}

func GetOrAddStreams(streams []StreamControl) []StreamControl {
	mapAccessMutex.Lock()
	defer mapAccessMutex.Unlock()

	for i, stream := range streams {
		streams[i], _ = GetOrAddStream(stream)
	}

	return streams
}

func TryRemoveStreams(streams ...StreamControl) {
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

	tryCopyExistingStreamToNewStream[T](newStream)

	addStream(newStream)

	return nil
}

func tryCopyExistingStreamToNewStream[T any](newStream Stream[T]) {
	if s, ok := streamIndex[newStream.ID()]; ok {
		newStream.setNotifiers(s.(Stream[T]).notifiers())
		newStream.setEvents(s.(Stream[T]).events())
	}
}

func addStream(newStream StreamControl) {
	streamIndex[newStream.ID()] = newStream
}

func validateStream(newStream StreamControl) error {

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
			return nil, StreamTypeMismatchError()
		}
	}
	return nil, StreamNotFoundError()

}

func Subscribe[T any](id StreamID) (*StreamReceiver[T], error) {
	mapAccessMutex.RLock()
	defer mapAccessMutex.RUnlock()

	if stream, err := getAndConvertStreamByID[T](id); err == nil {
		return stream.subscribe()
	} else {
		return nil, err
	}
}

func Unsubscribe[T any](rec *StreamReceiver[T]) {
	mapAccessMutex.RLock()
	defer mapAccessMutex.RUnlock()

	if rec == nil {
		return //TODO error?
	}

	if s, err := getAndConvertStreamByID[T](rec.StreamID); err == nil {
		s.unsubscribe(rec.ID)
	}
}

func RegisterPublisher[T any](id StreamID) (PublisherNew[T], error) {
	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s.newPublisher(), nil
	} else {
		return nil, err
	}
}

func UnRegisterPublisher[T any](publisher PublisherNew[T]) error {
	if s, err := getAndConvertStreamByID[T](publisher.StreamID()); err == nil {
		s.removePublisher(publisher.ID())
		return nil
	} else {
		return err
	}
}

func GetStreamByTopic[T any](topic string) (Stream[T], error) {
	return getAndConvertStreamByID[T](MakeStreamID[T](topic))
}

func GetStream[T any](id StreamID) (Stream[T], error) {
	return getAndConvertStreamByID[T](id)
}

func GetDescription(id StreamID) (StreamDescription, error) {
	if s, ok := streamIndex[id]; ok {
		return s.Description(), nil
	} else {
		return StreamDescription{}, StreamNotFoundError()
	}
}

func init() {
	streamIndex = make(map[StreamID]StreamControl)
}
