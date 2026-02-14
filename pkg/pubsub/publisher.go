package pubsub

import (
	"errors"
	"slices"

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
	}
	publisherManager[T any] interface {
		publishers() []*defaultPublisher[T]
		clearPublishers()
		closePublishers()
		addPublisher(pub *defaultPublisher[T])
		newPublisher(id StreamID, in publisherFanIn[T]) (Publisher[T], error)
		removePublisher(id PublisherID)
		len() int
		migratePublishers(newFanIn publisherFanIn[T], old publisherManager[T])
	}
	emptyPublisherFanIn[T any] struct {
	}
)

type defaultPublisherManager[T any] struct {
	publisherArr []*defaultPublisher[T]
}

func (p *defaultPublisherManager[T]) migratePublishers(newFanIn publisherFanIn[T], old publisherManager[T]) {
	for _, pub := range old.publishers() {
		pub.fanIn = newFanIn
		p.addPublisher(pub)
	}

	old.clearPublishers()
}

func (p *defaultPublisherManager[T]) publishers() []*defaultPublisher[T] {
	return p.publisherArr
}
func (p *defaultPublisherManager[T]) closePublishers() {
	// ensure no publisher is dangling
	for _, publisher := range p.publisherArr {
		if publisher != nil {
			publisher.fanIn = emptyPublisherFanIn[T]{}
		}
	}

	p.clearPublishers()
}

func (p *defaultPublisherManager[T]) clearPublishers() {
	p.publisherArr = make([]*defaultPublisher[T], 0)
}

func (p *defaultPublisherManager[T]) addPublisher(pub *defaultPublisher[T]) {
	p.publisherArr = append(p.publisherArr, pub)
}

func (p *defaultPublisherManager[T]) newPublisher(id StreamID, in publisherFanIn[T]) (Publisher[T], error) {
	publisher := newDefaultPublisher[T](id, in)
	p.publisherArr = append(p.publisherArr, publisher)

	return publisher, nil
}

func (p *defaultPublisherManager[T]) removePublisher(id PublisherID) {
	if idx := slices.IndexFunc(p.publisherArr, func(publisher *defaultPublisher[T]) bool { return id == publisher.ID() }); idx != -1 {
		p.publisherArr = append(p.publisherArr[:idx], p.publisherArr[idx+1:]...)
	}
}

func (p *defaultPublisherManager[T]) len() int {
	return len(p.publisherArr)
}

func (e emptyPublisherFanIn[T]) publish(events.Event[T]) error {
	return EmptyPublisherFanInPublisherError
}
func (e emptyPublisherFanIn[T]) publishC(T) error {
	return EmptyPublisherFanInPublisherError
}
func (e emptyPublisherFanIn[T]) lock()   {}
func (e emptyPublisherFanIn[T]) unlock() {}

func newDefaultPublisher[T any](streamID StreamID, fanIn publisherFanIn[T]) *defaultPublisher[T] {
	return &defaultPublisher[T]{
		id:       PublisherID(uuid.New()),
		streamID: streamID,
		fanIn:    fanIn,
	}
}

func newPublisherManager[T any]() publisherManager[T] {
	publishers := &defaultPublisherManager[T]{
		publisherArr: make([]*defaultPublisher[T], 0),
	}
	return publishers
}

func (p *defaultPublisher[T]) StreamID() StreamID {
	return p.streamID
}
func (p *defaultPublisher[T]) ID() PublisherID {
	return p.id
}
func (p *defaultPublisher[T]) PublishC(content T) error {
	e := events.NewEvent(content)
	return p.Publish(e)
}
func (p *defaultPublisher[T]) Publish(event events.Event[T]) error {
	return p.fanIn.publish(event)
}
