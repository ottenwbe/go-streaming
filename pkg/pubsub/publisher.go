package pubsub

import (
	"errors"
	"slices"
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/events"

	"github.com/google/uuid"
)

var (
	SinglePublisherFanInMoreThanOneError = errors.New("singlePublisherFanIn allows only one publisher")
	EmptyPublisherFanInPublisherError    = errors.New("emptyPublisherFanIn does not allow creation of publishers")
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
		streamID() StreamID
		len() int
		newPublisher() (Publisher[T], error)
		remove(id PublisherID)
		clear()
	}
	publisherFanInMutexSync[T any] struct {
		publishers []*defaultPublisher[T]
		stream     typedStream[T]
		mutex      sync.Mutex
	}
	singlePublisherFanIn[T any] struct {
		publisher *defaultPublisher[T]
		stream    typedStream[T]
	}
	emptyPublisherFanIn[T any] struct {
	}
)

func (e emptyPublisherFanIn[T]) publish(events.Event[T]) error {
	return nil
}

func (e emptyPublisherFanIn[T]) publishC(T) error {
	return nil
}

func (e emptyPublisherFanIn[T]) streamID() StreamID {
	//TODO implement me
	panic("implement me")
}

func (e emptyPublisherFanIn[T]) len() int {
	return 0
}

func (e emptyPublisherFanIn[T]) newPublisher() (Publisher[T], error) {
	return nil, EmptyPublisherFanInPublisherError
}

func (e emptyPublisherFanIn[T]) remove(PublisherID) {
}

func (e emptyPublisherFanIn[T]) clear() {
}

func (s *singlePublisherFanIn[T]) publish(event events.Event[T]) error {
	return s.stream.publish(event)
}

func (s *singlePublisherFanIn[T]) publishC(content T) error {

	event := events.NewEvent(content)

	return s.stream.publish(event)
}

func (s *singlePublisherFanIn[T]) streamID() StreamID {
	return s.stream.ID()
}

func (s *singlePublisherFanIn[T]) len() int {
	if s.publisher == nil {
		return 0
	}
	return 1
}

func (s *singlePublisherFanIn[T]) newPublisher() (Publisher[T], error) {
	if s.publisher == nil {
		s.publisher = newDefaultPublisher[T](s.streamID(), s)
		return s.publisher, nil
	}
	return nil, SinglePublisherFanInMoreThanOneError
}

func (s *singlePublisherFanIn[T]) remove(id PublisherID) {
	if s.publisher != nil && s.publisher.id == id {
		s.publisher = nil
	}
}

func (s *singlePublisherFanIn[T]) clear() {
	if s.publisher != nil {
		s.publisher.fanIn = emptyPublisherFanIn[T]{}
	}
	s.publisher = nil
}

func newDefaultPublisher[T any](streamID StreamID, fanIn publisherFanIn[T]) *defaultPublisher[T] {
	return &defaultPublisher[T]{
		id:       PublisherID(uuid.New()),
		streamID: streamID,
		fanIn:    fanIn,
	}
}

func newPublisherSync[T any](stream typedStream[T], singleFanIn bool) publisherFanIn[T] {
	if singleFanIn {
		return &singlePublisherFanIn[T]{
			publisher: nil,
			stream:    stream,
		}
	} else {
		return &publisherFanInMutexSync[T]{
			publishers: make([]*defaultPublisher[T], 0),
			stream:     stream,
		}
	}
}

func (p *publisherFanInMutexSync[T]) clear() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// ensure no publisher is dangling
	for _, publisher := range p.publishers {
		if publisher != nil {
			publisher.fanIn = emptyPublisherFanIn[T]{}
		}
	}

	p.publishers = make([]*defaultPublisher[T], 0)
}

func (p *publisherFanInMutexSync[T]) len() int {
	return len(p.publishers)
}

func (p *publisherFanInMutexSync[T]) streamID() StreamID {
	return p.stream.ID()
}

func (p *publisherFanInMutexSync[T]) publishC(content T) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	e := events.NewEvent(content)
	return p.stream.publish(e)
}

func (p *publisherFanInMutexSync[T]) publish(e events.Event[T]) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.stream.publish(e)
}

func (p *publisherFanInMutexSync[T]) newPublisher() (Publisher[T], error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	publisher := newDefaultPublisher[T](p.streamID(), p)
	p.publishers = append(p.publishers, publisher)

	return publisher, nil
}

func (p *publisherFanInMutexSync[T]) remove(publisherID PublisherID) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

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

func (p *defaultPublisher[T]) PublishC(content T) error {
	return p.fanIn.publishC(content)
}

func (p *defaultPublisher[T]) Publish(event events.Event[T]) error {
	return p.fanIn.publish(event)
}
