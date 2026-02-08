package query

import (
	"errors"

	"github.com/ottenwbe/go-streaming/internal/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	"github.com/google/uuid"
)

var nilContinuousError = errors.New("continuous error is empty")

// Builder helps construct a ContinuousQuery.
type Builder struct {
	q     *ContinuousQuery
	error []error
}

// ContinuousQuery represents a running query that processes streams.
type ContinuousQuery struct {
	id ID

	operators []engine.OperatorControl
	streams   []pubsub.StreamID
	output    pubsub.StreamID
}

// TypedContinuousQuery is a typed wrapper around ContinuousQuery that provides a typed output receiver.
type TypedContinuousQuery[T any] struct {
	*ContinuousQuery
	OutputReceiver pubsub.BatchSubscriber[T]
}

// Close stops the query and unsubscribes the output receiver.
func Close[T any](qs *TypedContinuousQuery[T]) {
	if qs == nil {
		return //TODO error
	}

	pubsub.Unsubscribe(qs.OutputReceiver)
	qs.close()
	qs = nil
}

// RunAndSubscribe starts the query and returns a typed wrapper with an active subscription to the output.
func RunAndSubscribe[T any](c *ContinuousQuery, err ...error) (*TypedContinuousQuery[T], []error) {

	err, done := anyErrorExists(err, c)
	if done {
		return nil, err
	}

	if runErr := c.run(); runErr != nil {
		c.close()
		return nil, append(err, runErr)
	}

	res, subErr := pubsub.SubscribeBatchByTopicID[T](c.output)
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

// Next waits for the next events from the query's output stream.
func (qs *TypedContinuousQuery[T]) Next() ([]events.Event[T], bool) {
	return qs.OutputReceiver.Next()
}

// ComposeWith merges another query into the current one, chaining their operations.
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

// ID returns the unique identifier of the query.
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

	c.startEverything()

	err := QueryRepository().put(c)
	return err
}

func newQueryControl(outStream pubsub.StreamID) *ContinuousQuery {
	return &ContinuousQuery{
		id:        ID(uuid.New()),
		operators: make([]engine.OperatorControl, 0),
		streams:   []pubsub.StreamID{},
		output:    outStream,
	}
}

func (c *ContinuousQuery) addStreams(streams ...pubsub.StreamID) {
	c.streams = append(c.streams, streams...)
}

func (c *ContinuousQuery) addOperations(operators ...engine.OperatorControl) {
	c.operators = append(c.operators, operators...)
}

func (c *ContinuousQuery) startEverything() {
	for _, operator := range c.operators {
		operator.Start()
	}
}

func (c *ContinuousQuery) stopEverything() {
	for _, operator := range c.operators {
		operator.Stop()
	}
}

func in(streams []pubsub.StreamID, id pubsub.StreamID) bool {
	for _, stream := range streams {
		if stream == id {
			return true
		}
	}
	return false
}

// NewBuilder creates a new query builder.
func NewBuilder() *Builder {
	return &Builder{
		q: &ContinuousQuery{
			id:        ID(uuid.New()),
			operators: make([]engine.OperatorControl, 0),
			streams:   make([]pubsub.StreamID, 0),
			output:    pubsub.NilStreamID(),
		},
		error: make([]error, 0),
	}
}

// S creates or retrieves a stream
// with the given configuration.
func S[T any](topic string, options ...pubsub.StreamOption) (pubsub.StreamID, error) {
	d := pubsub.MakeStreamDescription[T](topic, options...)
	return pubsub.AddOrReplaceStreamFromDescription[T](d)
}

// Query adds a sub-query to the builder.
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

// Stream adds a stream to the query being built.
func (b *Builder) Stream(s pubsub.StreamID, err error) *Builder {
	b.q.addStreams(s)
	if err != nil {
		b.error = append(b.error, err)
	}
	return b
}

// Errors returns any errors accumulated during the build process.
func (b *Builder) Errors() []error {
	return b.error
}

// Build constructs the final ContinuousQuery.
func (b *Builder) Build() (*ContinuousQuery, []error) {
	if len(b.error) > 0 {
		return nil, b.error
	}
	return b.q, nil
}
