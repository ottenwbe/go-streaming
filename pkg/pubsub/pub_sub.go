package pubsub

import (
	"errors"
	"go-stream-processing/pkg/events"
	"sync"
)

var (
	// streamIdx is the major dictionary to manage all streams locally
	streamIdx map[StreamID]Stream
	// streamIdxAccessMutex controls the access to streamIdx
	streamIdxAccessMutex sync.RWMutex
)

var (
	StreamTypeMismatchError = errors.New("pub sub: stream type mismatch")
	StreamNotFoundError     = errors.New("pub sub: no stream found")
	StreamIDNilError        = errors.New("pub sub: stream id nil")
	StreamRecNilError       = errors.New("pub sub: stream receiver nil")
)

// GetOrAddStream adds one streams to the pub sub system or returns an existing one.
func GetOrAddStream[T any](streamDescription StreamDescription) (Stream, error) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	stream := NewStreamFromDescription[T](streamDescription)
	return doGetOrAddStream(stream)
}

// GetOrAddStreams adds one or more streams to the pub sub system or returns an existing one.
// Note: Ignores Errors.
func GetOrAddStreams(streams ...Stream) []Stream {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	for i, stream := range streams {
		streams[i], _ = doGetOrAddStream(stream) // TODO: what about the error
	}

	return streams
}

// AddOrReplaceStreamFromDescription uses a description of a stream to add it or replace it to the pub sub system
func AddOrReplaceStreamFromDescription[T any](description StreamDescription) (Stream, error) {
	var (
		stream typedStream[T]
		err    error
	)

	stream = NewStreamFromDescription[T](description)
	err = AddOrReplaceStream(stream)

	return stream, err
}

// AddOrReplaceStream adds a new stream or replaces an existing one in the pub sub system.
// Replacing could be, for instance, changing the stream from synchronous to asynchronous,
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

// TryRemoveStreams attempts to remove the provided streams from the pub sub system.
// A stream is only removed if it has no active publishers or subscribers.
func TryRemoveStreams(streams ...Stream) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	for _, stream := range streams {
		if s, ok := streamIdx[stream.ID()]; ok && !s.HasPublishersOrSubscribers() {
			if s.TryClose() {
				delete(streamIdx, stream.ID())
			}

		}
	}
}

// Subscribe to a stream by the stream's id
func Subscribe[T any](id StreamID) (StreamReceiver[T], error) {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if stream, err := getAndConvertStreamByID[T](id); err == nil {
		return stream.subscribe()
	} else {
		return nil, err
	}
}

// Unsubscribe removes a subscriber from a stream.
func Unsubscribe[T any](rec StreamReceiver[T]) error {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if rec == nil {
		return StreamRecNilError
	}

	if s, err := getAndConvertStreamByID[T](rec.StreamID()); err == nil {
		s.unsubscribe(rec.ID())
		return nil
	} else {
		return err
	}
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

// RegisterPublisher creates and registers a new publisher for the stream identified by the given ID.
func RegisterPublisher[T any](id StreamID) (Publisher[T], error) {
	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s.newPublisher()
	} else {
		return nil, err
	}
}

// UnRegisterPublisher removes a publisher from its associated stream.
func UnRegisterPublisher[T any](publisher Publisher[T]) error {
	if s, err := getAndConvertStreamByID[T](publisher.StreamID()); err == nil {
		s.removePublisher(publisher.ID())
		return nil
	} else {
		return err
	}
}

// GetStreamByTopic retrieves a stream by its topic name.
func GetStreamByTopic[T any](topic string) (Stream, error) {
	return getAndConvertStreamByID[T](MakeStreamID[T](topic))
}

// GetStream retrieves a stream by its ID.
func GetStream(id StreamID) (Stream, error) {
	if s, ok := streamIdx[id]; ok {
		return s, nil
	}
	return nil, StreamNotFoundError
}

// GetDescription retrieves the description of a stream identified by the given ID.
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
