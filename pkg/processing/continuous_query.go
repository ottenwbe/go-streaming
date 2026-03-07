package processing

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var (
	ErrQueryNil        = errors.New("query: query cannot be nil")
	ErrInvalidCallback = errors.New("query: callback cannot be nil and needs to implement func(event events.Event[T])")
)

// ContinuousQuery represents a running query that processes streams.
type ContinuousQuery interface {
	addStreams(isSource bool, streams ...pubsub.StreamID)
	addOperations(operators ...OperatorID)
	sourceStreams() []pubsub.StreamID
	outputStream() pubsub.StreamID
	operatorIDs() []OperatorID
	ID() ID
	Close() error
	Run() error
	Subscribe(callback any, options ...pubsub.SubscriberOption) error
	out(from pubsub.StreamID)
	repository() *pubsub.StreamRepository
}

// TypedContinuousQuery is a typed wrapper around ContinuousQuery that provides a typed output receiver.
type TypedContinuousQuery[T any] struct {
	id ID

	operators []OperatorID
	streams   []pubsub.StreamID

	outStream pubsub.StreamID
	inStreams []pubsub.StreamID

	subscriptions []pubsub.TypedSubscriber[T]

	closeOnce sync.Once
	repo      *pubsub.StreamRepository
}

// newContinuousQuery creates a new, unstarted continuous query.
func newContinuousQuery[T any](opts ...QueryOption) ContinuousQuery {
	options := &queryOptions{
		repo: pubsub.DefaultStreamRepository(),
	}
	for _, opt := range opts {
		opt(options)
	}

	id := ID(uuid.New())
	return &TypedContinuousQuery[T]{
		id:        id,
		operators: make([]OperatorID, 0),
		streams:   []pubsub.StreamID{},
		repo:      options.repo,
	}
}

// Close stops the query and unsubscribes the output receiver.
func Close(qs ContinuousQuery) error {
	if qs == nil {
		return nil
	}
	return qs.Close()
}

// RegisterPublisher returns a publisher for a given source topic of the query.
// NOTE: remember to unregister!
func RegisterPublisher[T any](c ContinuousQuery, topic string) (pubsub.Publisher[T], error) {
	if c == nil {
		return nil, ErrQueryNil
	}

	streamID := pubsub.MakeStreamID[T](topic)

	if !in(c.sourceStreams(), streamID) {
		return nil, fmt.Errorf("topic %s not found in query sources", topic)
	}
	return pubsub.RegisterPublisherOnRepository[T](c.repository(), streamID)
}

// UnRegisterPublisher from query
func UnRegisterPublisher[T any](c ContinuousQuery, pub pubsub.Publisher[T]) error {
	if c == nil {
		return ErrQueryNil
	}
	return pubsub.UnRegisterPublisherOnRepository[T](c.repository(), pub)
}

// ID returns the unique identifier of the query.
func (c *TypedContinuousQuery[T]) ID() ID {
	return c.id
}

func (c *TypedContinuousQuery[T]) Subscribe(
	callback any,
	options ...pubsub.SubscriberOption,
) error {
	if cb, ok := callback.(func(event events.Event[T])); ok && cb != nil {
		s, err := pubsub.SubscribeByTopicIDOnRepository(c.repo, c.outStream, cb, options...)
		if err != nil {
			return err
		}
		c.subscriptions = append(c.subscriptions, s)
		return nil
	}
	return ErrInvalidCallback
}

func (c *TypedContinuousQuery[T]) Run() error {
	var err error
	for _, sid := range c.streams {
		if e := c.repo.StartStream(sid); e != nil {
			err = errors.Join(err, e)
		}
	}

	for _, oid := range c.operators {
		op, exists := OperatorRepository().Get(oid)
		if exists {
			if e := op.Start(); e != nil {
				err = errors.Join(err, e)
			}
		}
	}
	if err != nil {
		_ = c.Close()
		return err
	}
	return QueryRepository().put(c)
}

// close stops the query and all its components, cleaning up resources.
func (c *TypedContinuousQuery[T]) Close() error {
	var errs error
	c.closeOnce.Do(func() {
		for _, sub := range c.subscriptions {
			if err := pubsub.UnsubscribeOnRepository(c.repo, sub); err != nil {
				errs = errors.Join(errs, err)
			}
		}

		for _, o := range c.operators {
			if err := RemoveOperator(o); err != nil {
				errs = errors.Join(errs, err)
			}
		}
		c.repo.TryRemoveStreams(c.streams...)

		QueryRepository().remove(c.id)
	})
	return errs
}

// repository returns the stream repository associated with the query.
func (c *TypedContinuousQuery[T]) repository() *pubsub.StreamRepository {
	return c.repo
}

// sourceStreams returns the list of source stream IDs for the query.
func (c *TypedContinuousQuery[T]) sourceStreams() []pubsub.StreamID {
	return c.inStreams
}

// outputStream returns the main output stream ID of the query.
func (c *TypedContinuousQuery[T]) outputStream() pubsub.StreamID {
	return c.outStream
}

// operatorIDs returns the list of operator IDs in the query.
func (c *TypedContinuousQuery[T]) operatorIDs() []OperatorID {
	return c.operators
}

// addStreams adds stream IDs to the query's managed list.
func (c *TypedContinuousQuery[T]) addStreams(isSource bool, streams ...pubsub.StreamID) {
	c.streams = append(c.streams, streams...)
	if isSource {
		c.inStreams = append(c.inStreams, streams...)
	}
}

// addOperations adds operator IDs to the query's managed list.
func (c *TypedContinuousQuery[T]) addOperations(operators ...OperatorID) {
	c.operators = append(c.operators, operators...)
}

// out sets the main output stream for the query.
func (c *TypedContinuousQuery[T]) out(o pubsub.StreamID) {
	c.outStream = o
}

// in is a helper function to check if a stream ID exists in a slice of stream IDs.
func in(streams []pubsub.StreamID, id pubsub.StreamID) bool {
	for _, stream := range streams {
		if stream == id {
			return true
		}
	}
	return false
}
