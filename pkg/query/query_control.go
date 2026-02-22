package query

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	engine2 "github.com/ottenwbe/go-streaming/pkg/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var (
	ErrQueryNil        = errors.New("query: query cannot be nil")
	ErrInvalidCallback = errors.New("query: callback cannot be nil and needs to implement func(event events.Event[T])")
)

// ContinuousQuery represents a running query that processes streams.
type ContinuousQuery interface {
	addStreams(streams ...pubsub.StreamID)
	addOperations(operators ...engine2.OperatorID)
	ID() ID
	close()
	Run() error
	Subscribe(callback any, options ...pubsub.SubscriberOption) error
	out(from pubsub.StreamID)
	repository() *pubsub.StreamRepository
}

// TypedContinuousQuery is a typed wrapper around ContinuousQuery that provides a typed output receiver.
type TypedContinuousQuery[T any] struct {
	id ID

	operators []engine2.OperatorID
	streams   []pubsub.StreamID
	outStream pubsub.StreamID

	subscriptions []pubsub.Subscriber[T]
	closeOnce     sync.Once
	repo          *pubsub.StreamRepository
}

// Close stops the query and unsubscribes the output receiver.
func Close(qs ContinuousQuery) {
	if qs == nil {
		return
	}
	qs.close()
}

// ID returns the unique identifier of the query.
func (c *TypedContinuousQuery[T]) ID() ID {
	return c.id
}

func (c *TypedContinuousQuery[T]) repository() *pubsub.StreamRepository {
	return c.repo
}

func (c *TypedContinuousQuery[T]) out(o pubsub.StreamID) {
	c.outStream = o
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
		_ = c.repo.StartStream(sid)
	}

	for _, oid := range c.operators {
		op, exists := engine2.OperatorRepository().Get(oid)
		if exists {
			err = op.Start()
		}
	}
	if err != nil {
		return err
	}
	return QueryRepository().put(c)
}

func (c *TypedContinuousQuery[T]) close() {

	c.closeOnce.Do(func() {
		for _, sub := range c.subscriptions {
			pubsub.UnsubscribeOnRepository(c.repo, sub)
		}

		for _, o := range c.operators {
			engine2.RemoveOperator(o)
		}
		c.repo.TryRemoveStreams(c.streams...)

		QueryRepository().remove(c.id)
	})
}

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
		operators: make([]engine2.OperatorID, 0),
		streams:   []pubsub.StreamID{},
		repo:      options.repo,
	}
}

func (c *TypedContinuousQuery[T]) addStreams(streams ...pubsub.StreamID) {
	c.streams = append(c.streams, streams...)
}

func (c *TypedContinuousQuery[T]) addOperations(operators ...engine2.OperatorID) {
	c.operators = append(c.operators, operators...)
}

func in(streams []pubsub.StreamID, id pubsub.StreamID) bool {
	for _, stream := range streams {
		if stream == id {
			return true
		}
	}
	return false
}

func FromSourceStream[T any](topic string, options ...pubsub.StreamOption) func(q ContinuousQuery) StreamWError {

	return func(q ContinuousQuery) StreamWError {
		if q == nil {
			return StreamWError{pubsub.NilStreamID(), ErrQueryNil}
		}

		sid, err := pubsub.GetOrAddStreamOnRepository[T](q.repository(), topic, append(options, pubsub.WithAutoStart(false))...)
		if err != nil {
			return StreamWError{pubsub.NilStreamID(), err}
		}

		q.addStreams(sid)

		return StreamWError{sid, err}
	}
}

func Process[T any](
	operatorCreationFunc func(in []pubsub.StreamID, out []pubsub.StreamID) (engine2.OperatorID, error),
	fromF func(q ContinuousQuery) StreamWError,
	options ...pubsub.StreamOption,
) func(q ContinuousQuery) StreamWError {
	return func(q ContinuousQuery) StreamWError {

		if q == nil {
			return StreamWError{pubsub.NilStreamID(), ErrQueryNil}
		}
		from := fromF(q)

		to, err := pubsub.AddOrReplaceStreamOnRepository[T](q.repository(), uuid.New().String(), append(options, pubsub.WithAutoStart(false))...)
		if err != nil {
			return StreamWError{pubsub.NilStreamID(), err}
		}

		operatorEngine, err2 := operatorCreationFunc([]pubsub.StreamID{from.streamID}, []pubsub.StreamID{to})
		if err2 != nil {
			return StreamWError{pubsub.NilStreamID(), err2}
		}

		q.addOperations(operatorEngine)
		q.addStreams(to)

		return StreamWError{
			streamID: to,
			error:    nil,
		}
	}
}

func Query[T any](
	fromF func(q ContinuousQuery) StreamWError,
	opts ...QueryOption,
) (q ContinuousQuery, err error) {

	q = newContinuousQuery[T](opts...)

	from := fromF(q)
	if from.error == nil {
		q.out(from.streamID)
	}

	return q, from.error
}

func OnStream[T any](stream StreamWError) StreamWError {
	return stream
}

type StreamWError struct {
	streamID pubsub.StreamID
	error    error
}

type QueryOption func(*queryOptions)

type queryOptions struct {
	repo *pubsub.StreamRepository
}

func WithNewRepository() QueryOption {
	return func(o *queryOptions) {
		o.repo = pubsub.NewStreamRepository()
	}
}

func WithRepository(r *pubsub.StreamRepository) QueryOption {
	return func(o *queryOptions) {
		o.repo = r
	}
}
