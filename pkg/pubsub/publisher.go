package pubsub

import (
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

// Publisher routes events to a stream
type Publisher[T any] interface {
	// Publish an event to a stream with a given StreamID
	Publish(event events.Event[T]) error
	// ID that identifies this publisher
	ID() PublisherID
	// StreamID of the stream that an event of this publisher is published to
	StreamID() StreamID
}

type publisherSync[T any] interface {
	publish(event events.Event[T]) error
	streamID() StreamID
	len() int
	newPublisher() Publisher[T]
	remove(id PublisherID)
	clear()
}

type (
	publisherMapMutexSync[T any] struct {
		publishers []PublisherID
		stream     Stream[T]
		mutex      sync.Mutex
	}
)

func newDefaultPublisher[T any](streamID StreamID, pubSync publisherSync[T]) *defaultPublisher[T] {
	return &defaultPublisher[T]{
		id:           PublisherID(uuid.New()),
		streamID:     streamID,
		publisherMap: pubSync,
	}
}

func newPublisherSync[T any](stream Stream[T]) publisherSync[T] {
	return &publisherMapMutexSync[T]{
		publishers: make([]PublisherID, 0),
		stream:     stream,
	}
}

func (p *publisherMapMutexSync[T]) clear() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.publishers = make([]PublisherID, 0)
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

	publisher := newDefaultPublisher[T](p.streamID(), p)
	p.publishers = append(p.publishers, publisher.ID())

	return publisher
}

func (p *publisherMapMutexSync[T]) remove(publisherID PublisherID) {
	p.mutex.Lock()
	p.mutex.Unlock()

	if idx := slices.IndexFunc(p.publishers, func(pID PublisherID) bool { return publisherID == pID }); idx != -1 {
		p.publishers = append(p.publishers[:idx], p.publishers[idx+1:]...)
	}
}

type defaultPublisher[T any] struct {
	publisherMap publisherSync[T]
	id           PublisherID
	streamID     StreamID
}

func (p *defaultPublisher[T]) StreamID() StreamID {
	return p.streamID
}

func (p *defaultPublisher[T]) ID() PublisherID {
	return p.id
}

func (p *defaultPublisher[T]) Publish(event events.Event[T]) error {
	return p.publisherMap.publish(event)
}
