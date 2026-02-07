package pubsub

import (
	"errors"
	"slices"
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/events"

	"github.com/google/uuid"
)

var (
	EmptyPublisherFanInPublisherError = errors.New("emptyPublisherFanIn does not allow creation of publishers")
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
		Publish(event events.Event[T])
		// PublishC publishes content to a stream with a given StreamID
		PublishC(content T)
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
		close() error
		publish(event events.Event[T])
		publishC(content T)
		streamID() StreamID
		len() int
		newPublisher() (Publisher[T], error)
		remove(id PublisherID)
		copyFrom(publishers publisherFanIn[T])
		publishers() []*defaultPublisher[T]
	}
	defaultPublisherFanIn[T any] struct {
		publishersArr []*defaultPublisher[T]
		description   StreamDescription
		channel       events.EventChannel[T]
		metrics       *streamMetrics
		mutex         sync.RWMutex
	}
	emptyPublisherFanIn[T any] struct {
	}
)

func (e emptyPublisherFanIn[T]) publishers() []*defaultPublisher[T] {
	return make([]*defaultPublisher[T], 0)
}

func (e emptyPublisherFanIn[T]) copyFrom(publishers publisherFanIn[T]) {}

func (e emptyPublisherFanIn[T]) publish(events.Event[T]) {
	return
}

func (e emptyPublisherFanIn[T]) publishC(T) {
	return
}

func (e emptyPublisherFanIn[T]) streamID() StreamID {
	return NilStreamID()
}

func (e emptyPublisherFanIn[T]) len() int {
	return 0
}

func (e emptyPublisherFanIn[T]) newPublisher() (Publisher[T], error) {
	return nil, EmptyPublisherFanInPublisherError
}

func (e emptyPublisherFanIn[T]) remove(PublisherID) {
}

func (e emptyPublisherFanIn[T]) close() error { return nil }

func newDefaultPublisher[T any](streamID StreamID, fanIn publisherFanIn[T]) *defaultPublisher[T] {
	return &defaultPublisher[T]{
		id:       PublisherID(uuid.New()),
		streamID: streamID,
		fanIn:    fanIn,
	}
}

func newPublisherSync[T any](sDescription StreamDescription, eChannel events.EventChannel[T], metrics *streamMetrics) publisherFanIn[T] {
	return &defaultPublisherFanIn[T]{
		publishersArr: make([]*defaultPublisher[T], 0),
		description:   sDescription,
		channel:       eChannel,
		metrics:       metrics,
	}
}

func (p *defaultPublisherFanIn[T]) close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// ensure no publisher is dangling
	for _, publisher := range p.publishersArr {
		if publisher != nil {
			publisher.fanIn = emptyPublisherFanIn[T]{}
		}
	}

	p.publishersArr = make([]*defaultPublisher[T], 0)

	return nil
}

func (p *defaultPublisherFanIn[T]) len() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return len(p.publishersArr)
}

func (p *defaultPublisherFanIn[T]) streamID() StreamID {
	return p.description.ID
}

func (p *defaultPublisherFanIn[T]) publishC(content T) {
	p.publish(events.NewEvent(content))
}

func (p *defaultPublisherFanIn[T]) publish(event events.Event[T]) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	p.metrics.incNumEventsIn()

	p.channel <- event
}

func (p *defaultPublisherFanIn[T]) newPublisher() (Publisher[T], error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	publisher := newDefaultPublisher[T](p.streamID(), p)
	p.publishersArr = append(p.publishersArr, publisher)

	return publisher, nil
}

func (p *defaultPublisherFanIn[T]) remove(publisherID PublisherID) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if idx := slices.IndexFunc(p.publishersArr, func(publisher *defaultPublisher[T]) bool { return publisherID == publisher.ID() }); idx != -1 {
		p.publishersArr = append(p.publishersArr[:idx], p.publishersArr[idx+1:]...)
	}
}

func (p *defaultPublisherFanIn[T]) publishers() []*defaultPublisher[T] {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return p.publishersArr

}

func (p *defaultPublisherFanIn[T]) copyFrom(oldPublishers publisherFanIn[T]) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, pub := range oldPublishers.publishers() {
		pub.fanIn = p
		p.publishersArr = append(p.publishersArr, pub)
	}
}

func (p *defaultPublisher[T]) StreamID() StreamID {
	return p.streamID
}

func (p *defaultPublisher[T]) ID() PublisherID {
	return p.id
}

func (p *defaultPublisher[T]) PublishC(content T) {
	p.fanIn.publishC(content)
}
func (p *defaultPublisher[T]) Publish(event events.Event[T]) {
	p.fanIn.publish(event)
}
