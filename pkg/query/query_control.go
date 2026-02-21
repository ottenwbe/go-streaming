package query

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/internal/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var nilContinuousError = errors.New("continuous error is empty")

// ContinuousQuery represents a running query that processes streams.
type ContinuousQuery interface {
	addStreams(streams ...pubsub.StreamID)
	addOperations(operators ...engine.OperatorID)
	ID() ID
	close()
	Run()
	Subscribe(callback any, options ...pubsub.SubscriberOption) error
	out(from pubsub.StreamID)
}

// TypedContinuousQuery is a typed wrapper around ContinuousQuery that provides a typed output receiver.
type TypedContinuousQuery[T any] struct {
	id ID

	operators []engine.OperatorID
	streams   []pubsub.StreamID
	outStream pubsub.StreamID

	subscriptions []pubsub.Subscriber[T]
	closeOnce     sync.Once
}

// Close stops the query and unsubscribes the output receiver.
func Close(qs ContinuousQuery) {
	if qs == nil {
		return
	}
	qs.close()
}

// RunAndSubscribe starts the query and returns a typed wrapper with an active subscription to the output.
//func RunAndSubscribe[T any](c *ContinuousQuery, err ...error) (*TypedContinuousQuery[T], []error) {
//
//	errs, done := anyErrorExists(err, c)
//	if done {
//		return nil, errs
//	}
//
//	if runErr := c.run(); runErr != nil {
//		c.close()
//		return nil, append(errs, runErr)
//	}
//
//	return &TypedContinuousQuery[T]{
//		ContinuousQuery: c,
//	}, errs
//}
//
//func anyErrorExists(err []error, c *ContinuousQuery) ([]error, bool) {
//	for i, _ := range err {
//		if err[i] == nil {
//			err = append(err[:i], err[i+1:]...)
//		}
//	}
//
//	if c == nil {
//		err = append(err, nilContinuousError)
//	}
//
//	return err, len(err) > 0
//}

// ComposeWith merges another query into the current one, chaining their operations.
//func (c *TypedContinuousQuery[T]) ComposeWith(c2 *ContinuousQuery) (*ContinuousQuery, error) {
//
//	if !c2.output.IsNil() && in(c.streams, c2.output) {
//		c2.output = c.output
//	} else if (!c.output.IsNil() && in(c2.streams, c.output)) || (c.output.IsNil() && !c2.output.IsNil()) {
//		c.output = c2.output
//	} else {
//		return nil, errors.New("output streams don't match")
//	}
//
//	c.addStreams(c2.streams...)
//	c.addOperations(c2.operators...)
//
//	return c, nil
//}

// ID returns the unique identifier of the query.
func (c *TypedContinuousQuery[T]) ID() ID {
	return c.id
}

func (c *TypedContinuousQuery[T]) out(o pubsub.StreamID) {
	c.outStream = o
}

func (c *TypedContinuousQuery[T]) Subscribe(
	callback any,
	options ...pubsub.SubscriberOption,
) error {
	if cb, ok := callback.(func(event events.Event[T])); ok && cb != nil {
		s, err := pubsub.SubscribeByTopicID(c.outStream, cb, options...)
		if err != nil {
			return err
		}
		c.subscriptions = append(c.subscriptions, s)
		return nil
	}
	return errors.New("callback cannot be nil and needs to implement func(event events.Event[T])")
}

func (c *TypedContinuousQuery[T]) Run() {
	for _, oid := range c.operators {
		op, found := engine.OperatorRepository().Get(oid)
		if found {
			op.Start()
		}
	}
}

func (c *TypedContinuousQuery[T]) close() {

	c.closeOnce.Do(func() {
		for _, sub := range c.subscriptions {
			pubsub.Unsubscribe(sub)
		}

		for _, o := range c.operators {
			engine.RemoveOperator(o)
		}
		pubsub.TryRemoveStreams(c.streams...)

		QueryRepository().remove(c.id)
	})
}

func (c *TypedContinuousQuery[T]) run() error {

	var err error
	for _, oid := range c.operators {
		op, exists := engine.OperatorRepository().Get(oid)
		if exists {
			err = op.Start()
		}
	}

	if err != nil {
		return err
	}

	err = QueryRepository().put(c)
	return err
}

func newContinuousQuery[T any]() ContinuousQuery {
	id := ID(uuid.New())
	return &TypedContinuousQuery[T]{
		id:        id,
		operators: make([]engine.OperatorID, 0),
		streams:   []pubsub.StreamID{},
	}
}

func (c *TypedContinuousQuery[T]) addStreams(streams ...pubsub.StreamID) {
	c.streams = append(c.streams, streams...)
}

func (c *TypedContinuousQuery[T]) addOperations(operators ...engine.OperatorID) {
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
			return StreamWError{pubsub.NilStreamID(), errors.New("query cannot be nil")}
		}

		sid, err := pubsub.GetOrAddStream[T](topic, options...)
		if err != nil {
			return StreamWError{pubsub.NilStreamID(), err}
		}

		q.addStreams(sid)

		return StreamWError{sid, err}
	}
}

func Process[T any](
	operatorCreationFunc func(in []pubsub.StreamID, out []pubsub.StreamID) (engine.OperatorID, error),
	fromF func(q ContinuousQuery) StreamWError,
	options ...pubsub.StreamOption,
) func(q ContinuousQuery) StreamWError {
	return func(q ContinuousQuery) StreamWError {

		if q == nil {
			return StreamWError{pubsub.StreamID{}, errors.New("query cannot be nil")}
		}
		from := fromF(q)

		to, err := pubsub.AddOrReplaceStream[T](uuid.New().String(), options...)
		if err != nil {
			return StreamWError{pubsub.NilStreamID(), err}
		}

		operatorEngine, err2 := operatorCreationFunc([]pubsub.StreamID{from.streamID}, []pubsub.StreamID{to})
		if err2 != nil {
			return StreamWError{pubsub.NilStreamID(), err2}
		}

		q.addOperations(operatorEngine)

		return StreamWError{
			streamID: to,
			error:    nil,
		}
	}
}

func Query[T any](
	fromF func(q ContinuousQuery) StreamWError,
) (q ContinuousQuery, err error) {

	q = newContinuousQuery[T]()

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
