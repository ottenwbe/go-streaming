package processing

import (
	"errors"
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
	addStreams(streams ...pubsub.StreamID)
	addOperations(operators ...OperatorID)
	ID() ID
	close() error
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

	subscriptions []pubsub.Subscriber[T]
	closeOnce     sync.Once
	repo          *pubsub.StreamRepository
}

// Close stops the query and unsubscribes the output receiver.
func Close(qs ContinuousQuery) error {
	if qs == nil {
		return nil
	}
	return qs.close()
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
		_ = c.close()
		return err
	}
	return QueryRepository().put(c)
}

func (c *TypedContinuousQuery[T]) close() error {
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

func (c *TypedContinuousQuery[T]) addStreams(streams ...pubsub.StreamID) {
	c.streams = append(c.streams, streams...)
}

func (c *TypedContinuousQuery[T]) addOperations(operators ...OperatorID) {
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
