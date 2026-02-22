package pubsub

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var (
	ErrEmptyPublisherFanIn = errors.New("empty publisher cannot publish events")
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
		// Publish an event to a stream
		Publish(event events.Event[T]) error
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
		publish(event events.Event[T]) error
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
	publishersMap map[PublisherID]Publisher[T]
	mutex         sync.RWMutex
}

func (p *defaultPublisherManager[T]) migratePublishers(newFanIn publisherFanIn[T], old publisherManager[T]) {
	for _, pub := range old.publishers() {
		pub.setFanin(newFanIn)
		p.addPublisher(pub)
	}

	old.clearPublishers()
}

func (p *defaultPublisherManager[T]) publishers() []Publisher[T] {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	pubs := make([]Publisher[T], 0, len(p.publishersMap))
	for _, pub := range p.publishersMap {
		pubs = append(pubs, pub)
	}
	return pubs
}
func (p *defaultPublisherManager[T]) closePublishers() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	// ensure no publisher is dangling
	for _, publisher := range p.publishersMap {
		if publisher != nil {
			publisher.setFanin(emptyPublisherFanIn[T]{})
		}
	}

	p.publishersMap = make(map[PublisherID]Publisher[T])
}

func (p *defaultPublisherManager[T]) clearPublishers() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.publishersMap = make(map[PublisherID]Publisher[T])
}

func (p *defaultPublisherManager[T]) addPublisher(pub Publisher[T]) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.publishersMap[pub.ID()] = pub
}

func (p *defaultPublisherManager[T]) newPublisher(id StreamID, in publisherFanIn[T]) (Publisher[T], error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	publisher := newDefaultPublisher(id, in)
	p.publishersMap[publisher.ID()] = publisher

	return publisher, nil
}

func (p *defaultPublisherManager[T]) removePublisher(id PublisherID) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if pub, ok := p.publishersMap[id]; ok {
		pub.setFanin(emptyPublisherFanIn[T]{})
	}
	delete(p.publishersMap, id)
}

func (p *defaultPublisherManager[T]) len() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return len(p.publishersMap)
}

func (e emptyPublisherFanIn[T]) publish(event events.Event[T]) error {
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
	return &defaultPublisherManager[T]{
		publishersMap: make(map[PublisherID]Publisher[T]),
		mutex:         sync.RWMutex{},
	}
}

func (p *defaultPublisher[T]) StreamID() StreamID {
	return p.streamID
}
func (p *defaultPublisher[T]) ID() PublisherID {
	return p.id
}
func (p *defaultPublisher[T]) setFanin(fanIn publisherFanIn[T]) {
	p.fanIn = fanIn
}
func (p *defaultPublisher[T]) Publish(event events.Event[T]) error {
	return p.fanIn.publish(event)
}
