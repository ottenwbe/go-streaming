package query

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/internal/engine"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
)

var nilContinuousError = errors.New("continuous error is empty")

type Builder struct {
	q     *ContinuousQuery
	error []error
}

type ContinuousQuery struct {
	id ID

	operators []engine.OperatorControl
	streams   []pubsub.StreamControl
	output    pubsub.StreamID
}

type TypedContinuousQuery[T any] struct {
	*ContinuousQuery
	OutputReceiver pubsub.StreamReceiver[T]
}

func Close[T any](qs *TypedContinuousQuery[T]) {
	if qs == nil {
		return //TODO error
	}

	pubsub.Unsubscribe(qs.OutputReceiver)
	qs.close()
	qs = nil
}

func RunAndSubscribe[T any](c *ContinuousQuery, err ...error) (*TypedContinuousQuery[T], []error) {

	err, done := anyErrorExists(err, c)
	if done {
		return nil, err
	}

	if runErr := c.run(); runErr != nil {
		c.close()
		return nil, append(err, runErr)
	}

	res, subErr := pubsub.Subscribe[T](c.output)
	if subErr != nil {
		c.close()
		return nil, append(err, subErr)
	}

	return &TypedContinuousQuery[T]{
		ContinuousQuery: c,
		OutputReceiver:  res,
	}, err
}

func anyErrorExists(err []error, c *ContinuousQuery) ([]error, bool) {
	for i, _ := range err {
		if err[i] == nil {
			err = append(err[:i], err[i+1:]...)
		}
	}

	if c == nil {
		err = append(err, nilContinuousError)
	}

	return err, len(err) > 0
}

func (qs *TypedContinuousQuery[T]) Notify() (events.Event[T], bool) {
	e, ok := <-qs.OutputReceiver.Notify()
	return e, ok
}

func (c *ContinuousQuery) ComposeWith(c2 *ContinuousQuery) (*ContinuousQuery, error) {

	if !c2.output.IsNil() && in(c.streams, c2.output) {
		c2.output = c.output
	} else if (!c.output.IsNil() && in(c2.streams, c.output)) || (c.output.IsNil() && !c2.output.IsNil()) {
		c.output = c2.output
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

func (c *ContinuousQuery) close() {
	c.stopEverything()

	pubsub.TryRemoveStreams(c.streams...)
	engine.OperatorRepository().Remove(c.operators)

	QueryRepository().remove(c.id)
}

func (c *ContinuousQuery) run() error {

	c.streams = pubsub.GetOrAddStreams(c.streams)

	c.startEverything()

	err := QueryRepository().put(c)
	return err
}

func newQueryControl(outStream pubsub.StreamID) *ContinuousQuery {
	return &ContinuousQuery{
		id:        ID(uuid.New()),
		operators: make([]engine.OperatorControl, 0),
		streams:   []pubsub.StreamControl{},
		output:    outStream,
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

func NewBuilder() *Builder {
	return &Builder{
		q: &ContinuousQuery{
			id:        ID(uuid.New()),
			operators: make([]engine.OperatorControl, 0),
			streams:   make([]pubsub.StreamControl, 0),
			output:    pubsub.NilStreamID(),
		},
		error: make([]error, 0),
	}
}

func S[T any](topic string, async bool) (pubsub.StreamControl, error) {
	d := pubsub.MakeStreamDescription[T](topic, async)
	return pubsub.AddOrReplaceStreamD[T](d)
}

func (b *Builder) Query(q *ContinuousQuery, pErr error) *Builder {

	if pErr != nil {
		b.error = append(b.error, pErr)
	}

	var err error
	if b.q, err = b.q.ComposeWith(q); err != nil {
		b.error = append(b.error, err)
	}

	return b
}

func (b *Builder) Stream(s pubsub.StreamControl, err error) *Builder {
	b.q.addStreams(s)
	if err != nil {
		b.error = append(b.error, err)
	}
	return b
}

func (b *Builder) Errors() []error {
	return b.error
}

func (b *Builder) Build() (*ContinuousQuery, []error) {
	if len(b.error) > 0 {
		return nil, b.error
	}
	return b.q, nil
}
