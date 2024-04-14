package query

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/internal/engine"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
)

type ContinuousQuery struct {
	id        ID
	operators []engine.OperatorControl
	streams   []pubsub.StreamControl
	Output    pubsub.StreamID
}

type ResultSubscription[T any] struct {
	continuousQuery *ContinuousQuery
	receiver        *pubsub.StreamReceiver[T]
}

func (qs *ResultSubscription[T]) Notifier() events.EventChannel[T] {
	return qs.receiver.Notify
}

func (c *ContinuousQuery) ComposeWith(c2 *ContinuousQuery) (*ContinuousQuery, error) {

	if !c2.Output.IsNil() && in(c.streams, c2.Output) {
		c2.Output = c.Output
	} else if (!c.Output.IsNil() && in(c2.streams, c.Output)) || (c.Output.IsNil() && !c2.Output.IsNil()) {
		c.Output = c2.Output
	} else {
		return nil, errors.New("output streams don't match")
	}

	c.addStreams(c2.streams...)
	c.addOperations(c2.operators...)

	return c, nil
}

func (c *ContinuousQuery) ID() ID {
	return c.id
}

func Close[T any](qs *ResultSubscription[T]) {
	pubsub.Unsubscribe(qs.receiver)
	qs.continuousQuery.close()
	qs = nil
}

func (c *ContinuousQuery) close() {
	c.stopEverything()

	pubsub.TryRemoveStreams(c.streams)
	engine.OperatorRepository().Remove(c.operators)

	QueryRepository().remove(c.id)
}

func Run[T any](c *ContinuousQuery, err ...error) (*ResultSubscription[T], []error) {

	for i, _ := range err {
		if err[i] == nil {
			err = append(err[:i], err[i+1:]...)
		}
	}

	if len(err) > 0 {
		return nil, err
	}

	if runErr := c.run(); runErr != nil {
		return nil, append(err, runErr)
	}

	res, subErr := pubsub.Subscribe[T](c.Output)
	if subErr != nil {
		c.close()
		return nil, append(err, subErr)
	}

	return &ResultSubscription[T]{
		continuousQuery: c,
		receiver:        res,
	}, err
}

func (c *ContinuousQuery) run() error {
	c.startEverything()
	err := QueryRepository().put(c)
	return err
}

func newQueryControl(outStream pubsub.StreamID) *ContinuousQuery {
	return &ContinuousQuery{
		id:        ID(uuid.New()),
		operators: make([]engine.OperatorControl, 0),
		streams:   []pubsub.StreamControl{},
		Output:    outStream,
	}
}

func (c *ContinuousQuery) addStreams(streams ...pubsub.StreamControl) {
	c.streams = append(c.streams, streams...)
}

func (c *ContinuousQuery) addOperations(operators ...engine.OperatorControl) {
	c.operators = append(c.operators, operators...)
}

func (c *ContinuousQuery) startEverything() {
	for _, stream := range c.streams {
		stream.Run()
	}
	for _, operator := range c.operators {
		operator.Start()
	}
}

func (c *ContinuousQuery) stopEverything() {
	for _, operator := range c.operators {
		operator.Stop()
	}
	for _, stream := range c.streams {
		stream.TryClose()
	}
}

func in(streams []pubsub.StreamControl, id pubsub.StreamID) bool {
	for _, stream := range streams {
		if stream.ID() == id {
			return true
		}
	}
	return false
}
