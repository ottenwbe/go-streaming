package pubsub

import (
	"errors"
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/events"
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
func GetOrAddStream[T any](streamDescription StreamDescription) (StreamID, error) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	stream := newStreamFromDescription[T](streamDescription)
	return doGetOrAddStream(stream)
}

// AddOrReplaceStreamFromDescription uses a description of a stream to add it or replace it to the pub sub system
func AddOrReplaceStreamFromDescription[T any](description StreamDescription) (StreamID, error) {
	var (
		stream typedStream[T]
		err    error
	)

	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	stream = newStreamFromDescription[T](description)

	err = doAddOrReplaceStream(stream)
	return stream.ID(), err
}

// ForceRemoveStream forces removal of streams by their id, ensuring all resources are closed.
// The streams will be removed from the central pub sub system.
func ForceRemoveStream(streamIDs ...StreamID) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	for _, id := range streamIDs {
		if s, ok := streamIdx[id]; ok {
			s.forceClose()
			delete(streamIdx, id)
		}
	}
}

// TryRemoveStreams attempts to remove the provided streams from the pub sub system.
// A stream is only removed if it has no active publishers or subscribers.
func TryRemoveStreams(streamIDs ...StreamID) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	for _, id := range streamIDs {
		if s, ok := streamIdx[id]; ok && !s.HasPublishersOrSubscribers() {
			if s.TryClose() {
				delete(streamIdx, id)
			}

		}
	}
}

// SubscribeByTopic to get a stream for this topic with type T
func SubscribeByTopic[T any](topic string) (StreamReceiver[T], error) {
	return SubscribeByTopicID[T](MakeStreamID[T](topic))
}

// SubscribeByTopicID to a stream by the stream's id
func SubscribeByTopicID[T any](id StreamID) (StreamReceiver[T], error) {
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

	publisher, err := RegisterPublisher[T](MakeStreamID[T](topic))
	defer func(publisher Publisher[T]) {
		if publisher != nil {
			err = UnRegisterPublisher[T](publisher)
		}
	}(publisher)

	if err == nil {
		err = publisher.Publish(event)
		if err != nil {
			return err
		}
	}

	return err
}

// RegisterPublisherByTopic creates and registers a new publisher for the stream identified by the given topic.
func RegisterPublisherByTopic[T any](topic string) (Publisher[T], error) {
	return RegisterPublisher[T](MakeStreamID[T](topic))
}

// RegisterPublisher creates and registers a new publisher for the stream identified by the given ID.
func RegisterPublisher[T any](id StreamID) (Publisher[T], error) {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s.newPublisher()
	} else {
		return nil, err
	}
}

// UnRegisterPublisher removes a publisher from its associated stream.
func UnRegisterPublisher[T any](publisher Publisher[T]) error {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if publisher == nil {
		return nil
	}

	if s, err := getAndConvertStreamByID[T](publisher.StreamID()); err == nil {
		s.removePublisher(publisher.ID())
		return nil
	} else {
		return err
	}
}

// GetDescription retrieves the description of a stream identified by the given ID.
func GetDescription(id StreamID) (StreamDescription, error) {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()
	if s, ok := streamIdx[id]; ok {
		return s.Description(), nil
	} else {
		return StreamDescription{}, StreamNotFoundError
	}
}

func doGetOrAddStream(stream Stream) (StreamID, error) {
	err := validateStream(stream)
	if err != nil {
		return NilStreamID(), err
	}

	if existingStream, ok := streamIdx[stream.ID()]; ok {
		return existingStream.ID(), nil
	} else {
		addStream(stream)
		return stream.ID(), nil
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
	newStream.Run()
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
		switch stream := stream.(type) {
		case typedStream[T]:

			return stream, nil
		default:
			return nil, StreamTypeMismatchError
		}
	}
	return nil, StreamNotFoundError

}

func init() {
	streamIdx = make(map[StreamID]Stream)
}
