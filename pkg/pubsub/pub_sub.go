package pubsub

import (
	"errors"
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/events"
)

var (
	// streamIdx is the major dictionary to manage all streams locally
	streamIdx map[StreamID]stream
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
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	stream := newStreamFromDescription[T](description)

	return doAddOrReplaceStream(stream)
}

// ForceRemoveStream forces removal of streams by their id, ensuring all resources are closed.
// The streams will be removed from the central pub sub system.
// BE CAREFUL USING THIS: when active subscribers publishers exist, the code might panic.
// In most cases TryRemoveStream is the safer and better choice.
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

	doTryRemoveStreams(streamIDs...)
}

// SubscribeByTopic to get a stream for this topic with type T
func SubscribeByTopic[T any](topic string) (Subscriber[T], error) {
	return SubscribeByTopicID[T](MakeStreamID[T](topic))
}

// SubscribeByTopicID to a stream by the stream's id
func SubscribeByTopicID[T any](id StreamID) (Subscriber[T], error) {
	if stream, err := getOrAddStreamByID[T](id); err == nil {
		return stream.subscribe()
	} else {
		return nil, err
	}
}

// Unsubscribe removes a subscriber from a stream.
func Unsubscribe[T any](rec Subscriber[T]) error {
	var (
		autoCleanup = false
		id          StreamID
	)

	streamIdxAccessMutex.RLock()
	defer func() {
		streamIdxAccessMutex.RUnlock()
		if autoCleanup {
			TryRemoveStreams(id)
		}
	}()

	if rec == nil {
		return StreamRecNilError
	}

	s, err := getAndConvertStreamByID[T](rec.StreamID())
	if err != nil {
		return err
	}

	s.unsubscribe(rec.ID())
	autoCleanup = s.Description().AutoCleanup
	id = s.ID()

	return nil
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
		publisher.Publish(event)
	}

	return err
}

// RegisterPublisherByTopic creates and registers a new publisher for the stream identified by the given topic.
func RegisterPublisherByTopic[T any](topic string) (Publisher[T], error) {
	return RegisterPublisher[T](MakeStreamID[T](topic))
}

// RegisterPublisher creates and registers a new publisher for the stream identified by the given ID.
func RegisterPublisher[T any](id StreamID) (Publisher[T], error) {
	if s, err := getOrAddStreamByID[T](id); err == nil {
		return s.newPublisher()
	} else {
		return nil, err
	}
}

// UnRegisterPublisher removes a publisher from its associated stream.
func UnRegisterPublisher[T any](publisher Publisher[T]) error {
	var (
		autoCleanup = false
		id          StreamID
	)

	streamIdxAccessMutex.RLock()
	defer func() {
		streamIdxAccessMutex.RUnlock()
		if autoCleanup {
			TryRemoveStreams(id)
		}
	}()

	if publisher == nil {
		return nil
	}

	s, err := getAndConvertStreamByID[T](publisher.StreamID())
	if err != nil {
		return err
	}

	s.removePublisher(publisher.ID())
	id = s.ID()
	autoCleanup = s.Description().AutoCleanup

	return nil
}

// GetDescription retrieves the description of a stream identified by the given ID.
func GetDescription(id StreamID) (StreamDescription, error) {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if s, ok := streamIdx[id]; ok {
		return s.Description(), nil
	}

	return StreamDescription{}, StreamNotFoundError
}

// Metrics retrieves the metrics of a stream identified by the given ID.
func Metrics(id StreamID) (*StreamMetrics, error) {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if s, ok := streamIdx[id]; ok {
		return s.streamMetrics(), nil
	}

	return newStreamMetrics(), StreamNotFoundError
}

func doGetOrAddStream(stream stream) (StreamID, error) {
	err := validateStream(stream)
	if err != nil {
		return NilStreamID(), err
	}

	if existingStream, ok := streamIdx[stream.ID()]; ok {
		return existingStream.ID(), nil
	}

	addAndStartStream(stream)
	return stream.ID(), nil
}

func doAddOrReplaceStream(newStream stream) (StreamID, error) {

	err := validateStream(newStream)
	if err != nil {
		return NilStreamID(), err
	}

	if existingStream, ok := streamIdx[newStream.ID()]; ok {
		newStream.copyFrom(existingStream)
		doTryRemoveStreams(existingStream.ID())
	}

	addAndStartStream(newStream)

	return newStream.ID(), nil
}

func doTryRemoveStreams(streamIDs ...StreamID) {
	for _, id := range streamIDs {
		if s, ok := streamIdx[id]; ok && !s.hasPublishersOrSubscribers() {
			if s.tryClose() {
				delete(streamIdx, id)
			}
		}
	}
}

func addAndStartStream(newStream stream) {
	newStream.run()
	streamIdx[newStream.ID()] = newStream
}

func validateStream(newStream stream) error {
	if newStream.ID().IsNil() {
		return StreamIDNilError
	}
	return nil
}

func getOrAddStreamByID[T any](id StreamID) (typedStream[T], error) {
	streamIdxAccessMutex.RLock()
	s, err := getAndConvertStreamByID[T](id)
	streamIdxAccessMutex.RUnlock()

	if err == nil {
		return s, nil
	}
	if !errors.Is(err, StreamNotFoundError) {
		return nil, err
	}

	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s, nil
	}

	desc := MakeStreamDescriptionFromID(id, WithAutoCleanup(true))
	newS := newStreamFromDescription[T](desc)
	addAndStartStream(newS)

	return newS, nil
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
	streamIdx = make(map[StreamID]stream)
}
