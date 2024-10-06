package pubsub

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/pkg/events"
	"slices"
	"sync"
)

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
		// ID that identifies this publisher
		ID() PublisherID
		// StreamID of the stream that an event of this publisher is published to
		StreamID() StreamID
	}
	publisherMap[T any] interface {
		publish(event events.Event[T]) error
		streamID() StreamID
		len() int
		newPublisher() Publisher[T]
		remove(id PublisherID)
		clear()
	}
	defaultPublisher[T any] struct {
		publish  func(event events.Event[T]) error
		id       PublisherID
		streamID StreamID
	}
	publisherMapMutexSync[T any] struct {
		publishers []*defaultPublisher[T]
		stream     typedStream[T]
		mutex      sync.Mutex
	}
)

func newDefaultPublisher[T any](streamID StreamID, publish func(event events.Event[T]) error) *defaultPublisher[T] {
	return &defaultPublisher[T]{
		id:       PublisherID(uuid.New()),
		streamID: streamID,
		publish:  publish,
	}
}

func newPublisherSync[T any](stream typedStream[T]) publisherMap[T] {
	return &publisherMapMutexSync[T]{
		publishers: make([]*defaultPublisher[T], 0),
		stream:     stream,
	}
}

func (p *publisherMapMutexSync[T]) clear() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// ensure no publisher is dangling
	for _, publisher := range p.publishers {
		if publisher != nil {
			publisher.publish = emptyPublishFunc[T]()
		}
	}

	p.publishers = make([]*defaultPublisher[T], 0)
}

func emptyPublishFunc[T any]() func(event events.Event[T]) error {
	return func(event events.Event[T]) error {
		return errors.New("publisher: has been deleted")
	}
}

func (p *publisherMapMutexSync[T]) len() int {
	return len(p.publishers)
}

func (p *publisherMapMutexSync[T]) streamID() StreamID {
	return p.stream.ID()
}

func (p *publisherMapMutexSync[T]) publish(e events.Event[T]) error {
	p.mutex.Lock()
	p.mutex.Unlock()

	return p.stream.publish(e)
}

func (p *publisherMapMutexSync[T]) newPublisher() Publisher[T] {
	p.mutex.Lock()
	p.mutex.Unlock()

	publisher := newDefaultPublisher[T](p.streamID(), p.publish)
	p.publishers = append(p.publishers, publisher)

	return publisher
}

func (p *publisherMapMutexSync[T]) remove(publisherID PublisherID) {
	p.mutex.Lock()
	p.mutex.Unlock()

	if idx := slices.IndexFunc(p.publishers, func(publisher *defaultPublisher[T]) bool { return publisherID == publisher.ID() }); idx != -1 {
		p.publishers = append(p.publishers[:idx], p.publishers[idx+1:]...)
	}
}

func (p *defaultPublisher[T]) StreamID() StreamID {
	return p.streamID
}

func (p *defaultPublisher[T]) ID() PublisherID {
	return p.id
}

func (p *defaultPublisher[T]) Publish(event events.Event[T]) error {
	return p.publish(event)
}
