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
	StreamAlreadyExistsError = errors.New("pub sub: stream already exists")
	StreamTypeMismatchError  = errors.New("pub sub: stream type mismatch")
	StreamNotFoundError      = errors.New("pub sub: no stream found")
	StreamIDNilError         = errors.New("pub sub: stream id nil")
	StreamRecNilError        = errors.New("pub sub: stream receiver nil")
)

// GetOrAddStream adds one streams to the pub sub system or returns an existing one.
// Returns StreamAlreadyExistsError when stream already exists and existing stream is returned.
func GetOrAddStream[T any](topic string, opts ...StreamOption) (StreamID, error) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	streamDescription := MakeStreamDescription[T](topic, opts...)
	stream := newStreamFromDescription[T](streamDescription)
	return doGetOrAddStream[T](stream)
}

// AddOrReplaceStream uses a description of a stream to add it or replace it to the pub sub system
func AddOrReplaceStream[T any](topic string, opts ...StreamOption) (StreamID, error) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	streamDescription := MakeStreamDescription[T](topic, opts...)
	s, err := getAndConvertStreamByID[T](streamDescription.StreamID())

	// add new stream if no existing stream exists
	if errors.Is(err, StreamNotFoundError) {
		stream := newStreamFromDescription[T](streamDescription)
		return doGetOrAddStream[T](stream)
	} else if err != nil {
		return NilStreamID(), err
	}

	s.migrateStream(streamDescription)
	return s.ID(), nil
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
func SubscribeByTopic[T any](topic string, callback func(event events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	return SubscribeByTopicID[T](MakeStreamID[T](topic), callback, opts...)
}

// SubscribeBatchByTopic to get a stream for this topic with type T returning a batch subscriber
func SubscribeBatchByTopic[T any](topic string, callback func(events ...events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	return SubscribeBatchByTopicID[T](MakeStreamID[T](topic), callback, opts...)
}

// SubscribeByTopicID to a stream by the stream's id
func SubscribeByTopicID[T any](id StreamID, callback func(event events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	if stream, err := getOrAddStreamByID[T](id); err == nil {
		return stream.subscribe(callback, opts...)
	} else {
		return nil, err
	}
}

// SubscribeBatchByTopicID to a stream by the stream's id returning a batch subscriber
func SubscribeBatchByTopicID[T any](id StreamID, callback func(events ...events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

	if stream, err := getOrAddStreamByID[T](id); err == nil {
		return stream.subscribeBatch(callback, opts...)
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
func InstantPublishByTopic[T any](topic string, eventBody T) error {

	publisher, err := RegisterPublisherByTopic[T](topic)
	defer UnRegisterPublisher(publisher)
	if err != nil {
		return err
	}

	return publisher.Publish(eventBody)
}

// RegisterPublisherByTopic creates and registers a new publisher for the stream identified by the given topic.
func RegisterPublisherByTopic[T any](topic string) (Publisher[T], error) {
	return RegisterPublisher[T](MakeStreamID[T](topic))
}

// RegisterPublisher creates and registers a new publisher for the stream identified by the given ID.
func RegisterPublisher[T any](id StreamID) (Publisher[T], error) {
	streamIdxAccessMutex.Lock()
	defer streamIdxAccessMutex.Unlock()

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

// GetPublishersAndSubscribers retrieves the publisher and subscribers of the stream
//func GetPublishersAndSubscribers[T any](id StreamID) (publisherManager[T], subscribers[T], error) {
//	streamIdxAccessMutex.RLock()
//	defer streamIdxAccessMutex.RUnlock()
//
//	if s, err := getAndConvertStreamByID[T](id); err != nil {
//		return s.publishers(), s.subscribers(), nil
//	}
//
//	return nil, nil, StreamNotFoundError
//}

// Metrics retrieves the metrics of a stream identified by the given ID.
func Metrics(id StreamID) (*StreamMetrics, error) {
	streamIdxAccessMutex.RLock()
	defer streamIdxAccessMutex.RUnlock()

	if s, ok := streamIdx[id]; ok {
		return s.streamMetrics(), nil
	}

	return newStreamMetrics(), StreamNotFoundError
}

func doGetOrAddStream[T any](stream stream) (StreamID, error) {
	err := validateStream[T](stream)
	if err != nil {
		return NilStreamID(), err
	}

	if existingStream, ok := streamIdx[stream.ID()]; ok {
		return existingStream.ID(), StreamAlreadyExistsError
	}

	addAndStartStream(stream)
	return stream.ID(), nil
}

func doTryRemoveStreams(streamIDs ...StreamID) {
	for _, id := range streamIDs {
		if s, ok := streamIdx[id]; ok {
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

func validateStream[T any](newStream stream) error {
	if newStream.ID().IsNil() {
		return StreamIDNilError
	}
	testID := MakeStreamID[T](newStream.Description().ID.Topic)
	if testID != newStream.ID() {
		return StreamTypeMismatchError
	}
	return nil
}

func getOrAddStreamByID[T any](id StreamID) (typedStream[T], error) {
	s, err := getAndConvertStreamByID[T](id)
	if err == nil {
		return s, nil
	}
	if !errors.Is(err, StreamNotFoundError) {
		return nil, err
	}

	if s, err := getAndConvertStreamByID[T](id); err == nil {
		return s, nil
	}

	desc := MakeStreamDescriptionByID(id, WithAutoCleanup(true))
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
