package pubsub

import (
	"errors"
	"go-stream-processing/pkg/events"
	"sync"
)

var (
	streamIdx            map[StreamID]Stream
	streamIdxAccessMutex sync.RWMutex
)

var (
	StreamTypeMismatchError = errors.New("pub sub: stream type mismatch")
	StreamNotFoundError     = errors.New("pub sub: no stream found")
	StreamIDNilError        = errors.New("pub sub: stream id nil")
	StreamRecNilError       = errors.New("pub sub: stream receiver nil")
)

// GetOrAddStreamD adds one streams to the pub sub system or returns an existing one.
func GetOrAddStreamD[T any](streamDescription StreamDescription) (Stream, error) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	stream := NewStreamD[T](streamDescription)
	return doGetOrAddStream(stream)
}

// GetOrAddStreams adds one or more streams to the pub sub system or returns an existing one.
func GetOrAddStreams(streams ...Stream) []Stream {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	for i, stream := range streams {
		streams[i], _ = doGetOrAddStream(stream)
	}

	return streams
}

func AddOrReplaceStreamD[T any](description StreamDescription) (Stream, error) {
	var (
		stream typedStream[T]
		err    error
	)

	stream = NewStreamD[T](description)
	err = AddOrReplaceStream(stream)

	return stream, err
}

func AddOrReplaceStream(newStream Stream) error {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	return doAddOrReplaceStream(newStream)
}

// ForceRemoveStream takes StreamDescriptions and ensures that all resources are closed.
// The streams will be removed from the central pub sub system.
func ForceRemoveStream(streams ...StreamDescription) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	for _, stream := range streams {
		if s, ok := streamIdx[stream.ID]; ok {
			s.forceClose()
			delete(streamIdx, stream.ID)
		}
	}
}

func TryRemoveStreams(streams ...Stream) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	for _, stream := range streams {
		if s, ok := streamIdx[stream.ID()]; ok && !s.HasPublishersOrSubscribers() {
			s.TryClose()
			delete(streamIdx, stream.ID())
		}
	}

}

func Subscribe[T any](id StreamID) (StreamReceiver[T], error) {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if stream, err := getAndConvertStreamByID[T](id); err == nil {
		return stream.subscribe()
	} else {
		return nil, err
	}
}

func Unsubscribe[T any](rec StreamReceiver[T]) error {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if rec == nil {
		return StreamRecNilError
	}

	if s, err := getAndConvertStreamByID[T](rec.StreamID()); err == nil {
		s.unsubscribe(rec.ID())
	}

	return nil
}

// InstantPublishByTopic routes the event to the given topic, iff the stream exists.
// Note, this is only a helper function and for consistent streaming register a publisher; see RegisterPublisher.
func InstantPublishByTopic[T any](topic string, event events.Event[T]) (err error) {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	publisher, err := RegisterPublisher[T](MakeStreamID[T](topic))
	defer func(publisher Publisher[T]) { err = UnRegisterPublisher[T](publisher) }(publisher)

	if err == nil {
		err = publisher.Publish(event)
		if err != nil {
			return err
		}
	}

	return err
}

func RegisterPublisher[T any](id StreamID) (Publisher[T], error) {
	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s.newPublisher(), nil
	} else {
		return nil, err
	}
}

func UnRegisterPublisher[T any](publisher Publisher[T]) error {
	if s, err := getAndConvertStreamByID[T](publisher.StreamID()); err == nil {
		s.removePublisher(publisher.ID())
		return nil
	} else {
		return err
	}
}

func GetStreamByTopic[T any](topic string) (Stream, error) {
	return getAndConvertStreamByID[T](MakeStreamID[T](topic))
}

func GetStream(id StreamID) (Stream, error) {
	if s, ok := streamIdx[id]; ok {
		return s, nil
	}
	return nil, StreamNotFoundError
}

func GetDescription(id StreamID) (StreamDescription, error) {
	if s, ok := streamIdx[id]; ok {
		return s.Description(), nil
	} else {
		return StreamDescription{}, StreamNotFoundError
	}
}

func doGetOrAddStream(stream Stream) (Stream, error) {
	err := validateStream(stream)
	if err != nil {
		return stream, err
	}

	if existingStream, ok := streamIdx[stream.ID()]; ok {
		return existingStream, nil
	} else {
		addStream(stream)
		return stream, nil
	}
}

func doAddOrReplaceStream(newStream Stream) error {

	err := validateStream(newStream)
	if err != nil {
		return err
	}

	tryCopyExistingStreamToNewStream(newStream)
	addStream(newStream)

	return nil
}

func tryCopyExistingStreamToNewStream(newStream Stream) {
	if s, ok := streamIdx[newStream.ID()]; ok {
		newStream.copyFrom(s)
	}
}

func addStream(newStream Stream) {
	streamIdx[newStream.ID()] = newStream
}

func validateStream(newStream Stream) error {
	if newStream.ID().IsNil() {
		return StreamIDNilError
	}

	return nil
}

func getAndConvertStreamByID[T any](id StreamID) (typedStream[T], error) {

	if stream, ok := streamIdx[id]; ok {
		switch stream.(type) {
		case typedStream[T]:

			return stream.(typedStream[T]), nil
		default:
			return nil, StreamTypeMismatchError
		}
	}
	return nil, StreamNotFoundError

}

func init() {
	streamIdx = make(map[StreamID]Stream)
}
