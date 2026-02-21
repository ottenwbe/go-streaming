package pubsub

import (
	"errors"
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/events"
)

var (
	StreamAlreadyExistsError = errors.New("pub sub: stream already exists")
	StreamTypeMismatchError  = errors.New("pub sub: stream type mismatch")
	StreamNotFoundError      = errors.New("pub sub: no stream found")
	StreamIDNilError         = errors.New("pub sub: stream id nil")
	StreamRecNilError        = errors.New("pub sub: stream receiver nil")
)

// StreamRepository manages a collection of streams and their lifecycle.
type StreamRepository struct {
	streamIdx map[StreamID]stream
	mutex     sync.RWMutex
}

// NewRepository creates a new, isolated pub/sub instance.
func NewStreamRepository() *StreamRepository {
	return &StreamRepository{
		streamIdx: make(map[StreamID]stream),
	}
}

// defaultStreamRepository is the singleton instance used by global functions.
var defaultStreamRepository = NewStreamRepository()

// DefaultStreamRepository returns the singleton instance used by global functions.
func DefaultStreamRepository() *StreamRepository {
	return defaultStreamRepository
}

// GetOrAddStream adds one streams to the pub sub system or returns an existing one.
// Returns StreamAlreadyExistsError when stream already exists and existing stream is returned.
func GetOrAddStream[T any](topic string, opts ...StreamOption) (StreamID, error) {
	return GetOrAddStreamOnRepository[T](defaultStreamRepository, topic, opts...)
}

func GetOrAddStreamOnRepository[T any](b *StreamRepository, topic string, opts ...StreamOption) (StreamID, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	streamDescription := MakeStreamDescription[T](topic, opts...)
	stream := newStreamFromDescription[T](streamDescription)
	return b.doGetOrAddStream(stream)
}

// AddOrReplaceStream uses a description of a stream to add it or replace it to the pub sub system
func AddOrReplaceStream[T any](topic string, opts ...StreamOption) (StreamID, error) {
	return AddOrReplaceStreamOnRepository[T](defaultStreamRepository, topic, opts...)
}

func AddOrReplaceStreamOnRepository[T any](b *StreamRepository, topic string, opts ...StreamOption) (StreamID, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	streamDescription := MakeStreamDescription[T](topic, opts...)
	s, err := getAndConvertStreamByID[T](b, streamDescription.StreamID())

	// add new stream if no existing stream exists
	if errors.Is(err, StreamNotFoundError) {
		stream := newStreamFromDescription[T](streamDescription)
		return b.doGetOrAddStream(stream)
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
	defaultStreamRepository.ForceRemoveStream(streamIDs...)
}

func (r *StreamRepository) ForceRemoveStream(streamIDs ...StreamID) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, id := range streamIDs {
		if s, ok := r.streamIdx[id]; ok {
			s.forceClose()
			delete(r.streamIdx, id)
		}
	}
}

// TryRemoveStreams attempts to remove the provided streams from the pub sub system.
// A stream is only removed if it has no active publishers or subscribers.
func TryRemoveStreams(streamIDs ...StreamID) {
	defaultStreamRepository.TryRemoveStreams(streamIDs...)
}

func (r *StreamRepository) TryRemoveStreams(streamIDs ...StreamID) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.doTryRemoveStreams(streamIDs...)
}

// StartStream starts the stream with the given ID.
func StartStream(id StreamID) error {
	return defaultStreamRepository.StartStream(id)
}

func (r *StreamRepository) StartStream(id StreamID) error {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	if s, ok := r.streamIdx[id]; ok {
		s.run()
		return nil
	}
	return StreamNotFoundError
}

// SubscribeByTopic to get a stream for this topic with type T
func SubscribeByTopic[T any](topic string, callback func(event events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	return SubscribeByTopicOnRepository[T](defaultStreamRepository, topic, callback, opts...)
}

func SubscribeByTopicOnRepository[T any](r *StreamRepository, topic string, callback func(event events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	return SubscribeByTopicIDOnRepository[T](r, MakeStreamID[T](topic), callback, opts...)
}

// SubscribeBatchByTopic to get a stream for this topic with type T returning a batch subscriber
func SubscribeBatchByTopic[T any](topic string, callback func(events ...events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	return SubscribeBatchByTopicOnRepository[T](defaultStreamRepository, topic, callback, opts...)
}

func SubscribeBatchByTopicOnRepository[T any](r *StreamRepository, topic string, callback func(events ...events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	return SubscribeBatchByTopicIDOnRepository[T](r, MakeStreamID[T](topic), callback, opts...)
}

// SubscribeByTopicID to a stream by the stream's id
func SubscribeByTopicID[T any](id StreamID, callback func(event events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	return SubscribeByTopicIDOnRepository[T](defaultStreamRepository, id, callback, opts...)
}

func SubscribeByTopicIDOnRepository[T any](b *StreamRepository, id StreamID, callback func(event events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if stream, err := getOrAddStreamByID[T](b, id); err == nil {
		return stream.subscribe(callback, opts...)
	} else {
		return nil, err
	}
}

// SubscribeBatchByTopicID to a stream by the stream's id returning a batch subscriber
func SubscribeBatchByTopicID[T any](id StreamID, callback func(events ...events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	return SubscribeBatchByTopicIDOnRepository[T](defaultStreamRepository, id, callback, opts...)
}

func SubscribeBatchByTopicIDOnRepository[T any](b *StreamRepository, id StreamID, callback func(events ...events.Event[T]), opts ...SubscriberOption) (Subscriber[T], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if stream, err := getOrAddStreamByID[T](b, id); err == nil {
		return stream.subscribeBatch(callback, opts...)
	} else {
		return nil, err
	}
}

// Unsubscribe removes a subscriber from a stream.
func Unsubscribe[T any](rec Subscriber[T]) error {
	return UnsubscribeOnRepository[T](defaultStreamRepository, rec)
}

func UnsubscribeOnRepository[T any](b *StreamRepository, rec Subscriber[T]) error {
	var (
		autoCleanup = false
		id          StreamID
	)

	b.mutex.RLock()
	defer func() {
		b.mutex.RUnlock()
		if autoCleanup {
			b.TryRemoveStreams(id)
		}
	}()

	if rec == nil {
		return StreamRecNilError
	}

	s, err := getAndConvertStreamByID[T](b, rec.StreamID())
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
	return InstantPublishByTopicOnRepository[T](defaultStreamRepository, topic, eventBody)
}

func InstantPublishByTopicOnRepository[T any](b *StreamRepository, topic string, eventBody T) error {
	publisher, err := RegisterPublisherByTopicOnRepository[T](b, topic)
	defer UnRegisterPublisherOnRepository[T](b, publisher)
	if err != nil {
		return err
	}

	return publisher.Publish(eventBody)
}

// RegisterPublisherByTopic creates and registers a new publisher for the stream identified by the given topic.
func RegisterPublisherByTopic[T any](topic string) (Publisher[T], error) {
	return RegisterPublisherOnRepository[T](defaultStreamRepository, MakeStreamID[T](topic))
}

func RegisterPublisherByTopicOnRepository[T any](b *StreamRepository, topic string) (Publisher[T], error) {
	return RegisterPublisherOnRepository[T](b, MakeStreamID[T](topic))
}

// RegisterPublisher creates and registers a new publisher for the stream identified by the given ID.
func RegisterPublisher[T any](id StreamID) (Publisher[T], error) {
	return RegisterPublisherOnRepository[T](defaultStreamRepository, id)
}

func RegisterPublisherOnRepository[T any](b *StreamRepository, id StreamID) (Publisher[T], error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if s, err := getOrAddStreamByID[T](b, id); err == nil {
		return s.newPublisher()
	} else {
		return nil, err
	}
}

// UnRegisterPublisher removes a publisher from its associated stream.
func UnRegisterPublisher[T any](publisher Publisher[T]) error {
	return UnRegisterPublisherOnRepository[T](defaultStreamRepository, publisher)
}

func UnRegisterPublisherOnRepository[T any](b *StreamRepository, publisher Publisher[T]) error {
	var (
		autoCleanup = false
		id          StreamID
	)

	b.mutex.RLock()
	defer func() {
		b.mutex.RUnlock()
		if autoCleanup {
			b.TryRemoveStreams(id)
		}
	}()

	if publisher == nil {
		return nil
	}

	s, err := getAndConvertStreamByID[T](b, publisher.StreamID())
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
	return defaultStreamRepository.GetDescription(id)
}

func (r *StreamRepository) GetDescription(id StreamID) (StreamDescription, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	if s, ok := r.streamIdx[id]; ok {
		return s.Description(), nil
	}

	return StreamDescription{}, StreamNotFoundError
}

// GetPublishersAndSubscribers retrieves the publisher and subscribers of the stream
//func GetPublishersAndSubscribers[T any](id StreamID) (publisherManager[T], subscribers[T], error) {
//	b.mutex.RLock()
//	defer b.mutex.RUnlock()
//
//	if s, err := b.getAndConvertStreamByID(id); err != nil {
//		return s.publishers(), s.subscribers(), nil
//	}
//
//	return nil, nil, StreamNotFoundError
//}

// Metrics retrieves the metrics of a stream identified by the given ID.
func Metrics(id StreamID) (*StreamMetrics, error) {
	return defaultStreamRepository.Metrics(id)
}

func (r *StreamRepository) Metrics(id StreamID) (*StreamMetrics, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	if s, ok := r.streamIdx[id]; ok {
		return s.streamMetrics(), nil
	}

	return newStreamMetrics(), StreamNotFoundError
}

func (r *StreamRepository) doGetOrAddStream(stream stream) (StreamID, error) {
	err := validateStream(stream)
	if err != nil {
		return NilStreamID(), err
	}

	if existingStream, ok := r.streamIdx[stream.ID()]; ok {
		return existingStream.ID(), StreamAlreadyExistsError
	}

	r.addAndStartStream(stream)
	return stream.ID(), nil
}

func (r *StreamRepository) doTryRemoveStreams(streamIDs ...StreamID) {
	for _, id := range streamIDs {
		if s, ok := r.streamIdx[id]; ok {
			if s.tryClose() {
				delete(r.streamIdx, id)
			}
		}
	}
}

func (r *StreamRepository) addAndStartStream(newStream stream) {
	if newStream.Description().AutoStart {
		newStream.run()
	}
	r.streamIdx[newStream.ID()] = newStream
}

func validateStream(newStream stream) error {
	if newStream.ID().IsNil() {
		return StreamIDNilError
	}
	// Type validation is implicit via StreamID equality in GetOrAdd logic or explicit checks in getAndConvert
	return nil
}

func getOrAddStreamByID[T any](b *StreamRepository, id StreamID) (typedStream[T], error) {
	s, err := getAndConvertStreamByID[T](b, id)
	if err == nil {
		return s, nil
	}
	if !errors.Is(err, StreamNotFoundError) {
		return nil, err
	}

	if s, err := getAndConvertStreamByID[T](b, id); err == nil {
		return s, nil
	}

	desc := MakeStreamDescriptionByID(id, WithAutoCleanup(true))
	newS := newStreamFromDescription[T](desc)
	b.addAndStartStream(newS)

	return newS, nil
}

func getAndConvertStreamByID[T any](b *StreamRepository, id StreamID) (typedStream[T], error) {
	if stream, ok := b.streamIdx[id]; ok {
		switch stream := stream.(type) {
		case typedStream[T]:
			return stream, nil
		default:
			return nil, StreamTypeMismatchError
		}
	}
	return nil, StreamNotFoundError

}
