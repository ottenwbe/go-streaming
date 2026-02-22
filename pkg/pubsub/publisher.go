package pubsub

import (
	"errors"
	"slices"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var (
	ErrEmptyPublisherFanIn = errors.New("empty Publisher cannot publishSource events")
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
		Publish(eventBody T) error
		// PublishComplex event to the stream
		PublishComplex(event events.Event[T]) error
		// ID that identifies this publisher
		ID() PublisherID
		// StreamID of the stream that an event of this publisher is published to
		StreamID() StreamID
		// setFanin of the publisher
		setFanin(fanin publisherFanIn[T])
	}
	defaultPublisher[T any] struct {
		fanIn    publisherFanIn[T]
		id       PublisherID
		streamID StreamID
	}
)

type (
	publisherFanIn[T any] interface {
		publishSource(content T) error
		publishComplex(event events.Event[T]) error
	}
	publisherManager[T any] interface {
		publishers() []Publisher[T]
		clearPublishers()
		closePublishers()
		addPublisher(pub Publisher[T])
		newPublisher(id StreamID, in publisherFanIn[T]) (Publisher[T], error)
		removePublisher(id PublisherID)
		len() int
		migratePublishers(newFanIn publisherFanIn[T], old publisherManager[T])
	}
	emptyPublisherFanIn[T any] struct {
	}
)

type defaultPublisherManager[T any] struct {
	publisherArr []Publisher[T]
}

func (p *defaultPublisherManager[T]) migratePublishers(newFanIn publisherFanIn[T], old publisherManager[T]) {
	for _, pub := range old.publishers() {
		pub.setFanin(newFanIn)
		p.addPublisher(pub)
	}

	old.clearPublishers()
}

func (p *defaultPublisherManager[T]) publishers() []Publisher[T] {
	return p.publisherArr
}
func (p *defaultPublisherManager[T]) closePublishers() {
	// ensure no publisher is dangling
	for _, publisher := range p.publisherArr {
		if publisher != nil {
			publisher.setFanin(emptyPublisherFanIn[T]{})
		}
	}

	p.clearPublishers()
}

func (p *defaultPublisherManager[T]) clearPublishers() {
	for i := range p.publisherArr {
		p.publisherArr[i] = nil
	}
	p.publisherArr = p.publisherArr[:0]
}

func (p *defaultPublisherManager[T]) addPublisher(pub Publisher[T]) {
	p.publisherArr = append(p.publisherArr, pub)
}

func (p *defaultPublisherManager[T]) newPublisher(id StreamID, in publisherFanIn[T]) (Publisher[T], error) {
	publisher := newDefaultPublisher(id, in)
	p.publisherArr = append(p.publisherArr, publisher)

	return publisher, nil
}

func (p *defaultPublisherManager[T]) removePublisher(id PublisherID) {
	if idx := slices.IndexFunc(p.publisherArr, func(publisher Publisher[T]) bool { return id == publisher.ID() }); idx != -1 {
		copy(p.publisherArr[idx:], p.publisherArr[idx+1:])
		p.publisherArr[len(p.publisherArr)-1] = nil
		p.publisherArr = p.publisherArr[:len(p.publisherArr)-1]
	}
}

func (p *defaultPublisherManager[T]) len() int {
	return len(p.publisherArr)
}

func (e emptyPublisherFanIn[T]) publishSource(T) error {
	return ErrEmptyPublisherFanIn
}
func (e emptyPublisherFanIn[T]) publishComplex(event events.Event[T]) error {
	return ErrEmptyPublisherFanIn
}

func newDefaultPublisher[T any](streamID StreamID, fanIn publisherFanIn[T]) *defaultPublisher[T] {
	return &defaultPublisher[T]{
		id:       PublisherID(uuid.New()),
		streamID: streamID,
		fanIn:    fanIn,
	}
}

func newPublisherManager[T any]() publisherManager[T] {
	publishers := &defaultPublisherManager[T]{
		publisherArr: make([]Publisher[T], 0),
	}
	return publishers
}

func (p *defaultPublisher[T]) StreamID() StreamID {
	return p.streamID
}
func (p *defaultPublisher[T]) ID() PublisherID {
	return p.id
}
func (p *defaultPublisher[T]) Publish(eventBody T) error {
	return p.fanIn.publishSource(eventBody)
}
func (p *defaultPublisher[T]) setFanin(fanIn publisherFanIn[T]) {
	p.fanIn = fanIn
}
func (p *defaultPublisher[T]) PublishComplex(event events.Event[T]) error {
	return p.fanIn.publishComplex(event)
}
