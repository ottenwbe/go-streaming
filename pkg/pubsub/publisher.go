package pubsub

import (
	"errors"

	"github.com/ottenwbe/go-streaming/pkg/events"

	"github.com/google/uuid"
)

var (
	EmptyPublisherFanInPublisherError = errors.New("empty Publisher cannot publish events")
)

// PublisherID uniquely identifies a publisher.
type PublisherID uuid.UUID

// String representation of the PublisherID
func (p PublisherID) String() string {
	return uuid.UUID(p).String()
}

type (
	// Publisher routes events to a stream
	Publisher[T any] interface {
		// Publish an event to a stream with a given StreamID
		Publish(event events.Event[T]) error
		// PublishC publishes content to a stream with a given StreamID
		PublishC(content T) error
		// ID that identifies this publisher
		ID() PublisherID
		// StreamID of the stream that an event of this publisher is published to
		StreamID() StreamID
	}
	defaultPublisher[T any] struct {
		fanIn    publisherFanIn[T]
		id       PublisherID
		streamID StreamID
	}
)
type (
	publisherFanIn[T any] interface {
		publish(event events.Event[T]) error
		publishC(content T) error
		publishers() publisherArr[T]
		clearPublishers()
		addPublisher(pub *defaultPublisher[T])
		newPublisher() (Publisher[T], error)
		removePublisher(id PublisherID)
	}
	emptyPublisherFanIn[T any] struct {
	}
)

type publisherArr[T any] []*defaultPublisher[T]

func (e emptyPublisherFanIn[T]) publish(events.Event[T]) error {
	return EmptyPublisherFanInPublisherError
}
func (e emptyPublisherFanIn[T]) publishC(T) error {
	return EmptyPublisherFanInPublisherError
}
func (e emptyPublisherFanIn[T]) publishers() publisherArr[T]       { return nil }
func (e emptyPublisherFanIn[T]) clearPublishers()                  {}
func (e emptyPublisherFanIn[T]) addPublisher(*defaultPublisher[T]) {}
func (e emptyPublisherFanIn[T]) newPublisher() (Publisher[T], error) {
	return nil, EmptyPublisherFanInPublisherError
}
func (e emptyPublisherFanIn[T]) removePublisher(id PublisherID) {}

func newDefaultPublisher[T any](streamID StreamID, fanIn publisherFanIn[T]) *defaultPublisher[T] {
	return &defaultPublisher[T]{
		id:       PublisherID(uuid.New()),
		streamID: streamID,
		fanIn:    fanIn,
	}
}

func newPublisherFanIn[T any]() publisherArr[T] {
	publishers := make([]*defaultPublisher[T], 0)
	return publishers
}

func (p publisherArr[T]) len() int {
	return len(p)
}

func (p *defaultPublisher[T]) StreamID() StreamID {
	return p.streamID
}
func (p *defaultPublisher[T]) ID() PublisherID {
	return p.id
}
func (p *defaultPublisher[T]) PublishC(content T) error {
	return p.fanIn.publishC(content)
}
func (p *defaultPublisher[T]) Publish(event events.Event[T]) error {
	return p.fanIn.publish(event)
}
