package query

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/internal/engine"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
)

type QueryDescription struct {
	operators []engine.OperatorControl
	streams   map[pubsub.StreamID]pubsub.StreamDescription
	output    pubsub.StreamID
}

func (q *QueryDescription) AddOutput(outputID pubsub.StreamID) {
	q.output = outputID
}

func (q *QueryDescription) AddStreams(stream pubsub.StreamDescription) {
	if _, ok := q.streams[stream.ID]; !ok {
		q.streams[stream.StreamID()] = stream
	}
}

func (q *QueryDescription) Streams() map[pubsub.StreamID]pubsub.StreamDescription {
	return q.streams
}

func (q *QueryDescription) Operators() []engine.OperatorControl {
	return q.operators
}

func (q *QueryDescription) AddOperator(operator engine.OperatorControl) {
	q.operators = append(q.operators, operator)
}

func NewQueryDescription() *QueryDescription {
	return &QueryDescription{
		operators: make([]engine.OperatorControl, 0),
		streams:   make(map[pubsub.StreamID]pubsub.StreamDescription),
		output:    pubsub.NilStreamID(),
	}
}

type ContinuousQuery struct {
	id ID

	operators []engine.OperatorControl
	streams   []pubsub.StreamControl
	Output    pubsub.StreamID
}

type Builder struct {
	q *ContinuousQuery

	error []error
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

func NewBuilder() *Builder {
	return &Builder{
		q: &ContinuousQuery{
			id:        ID(uuid.New()),
			operators: make([]engine.OperatorControl, 0),
			streams:   make([]pubsub.StreamControl, 0),
			Output:    pubsub.NilStreamID(),
		},
		error: make([]error, 0),
	}
}

func S[T any](id pubsub.StreamID, async bool) (pubsub.StreamControl, error) {
	d := pubsub.MakeStreamDescription(id, async)
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
