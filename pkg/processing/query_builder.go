package processing

import (
	"errors"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var (
	ErrNilStream       = errors.New("builder: stream cannot be nil")
	ErrOutputUndefined = errors.New("builder: output undefined")
	ErrEmptyInput      = errors.New("builder: operator requires at least one input stream")
	ErrAmbiguousOutput = errors.New("builder: query results in multiple output streams, cannot determine main output")
)

// Default Builder (Builder1):

// Builder provides an API to construct ContinuousQueries.
type Builder struct {
	streams   map[pubsub.StreamID]CreateStreamFunc
	operators map[OperatorID]func() (OperatorID, error)

	current []pubsub.StreamID
	query   ContinuousQuery

	err error
}

// NewBuilder creates a new query builder.
func NewBuilder[T any](opts ...QueryOption) *Builder {
	return &Builder{
		query:     newContinuousQuery[T](opts...),
		streams:   make(map[pubsub.StreamID]CreateStreamFunc),
		operators: make(map[OperatorID]func() (OperatorID, error)),
	}
}

// CreateStreamFunc encapsulates the creation logic for a source stream.
type CreateStreamFunc func(repo *pubsub.StreamRepository) (pubsub.StreamID, error)
type StreamCreationOptions struct {
	streamCreateFunc CreateStreamFunc
	streamID         pubsub.StreamID
}

type CreateOperatorFunc func([]pubsub.StreamID, []pubsub.StreamID, OperatorID) (OperatorID, error)
type OperatorCreationOptions struct {
	outStreamCreateOptions []StreamCreationOptions
	operatorCreateFunc     CreateOperatorFunc
	operatorID             OperatorID
}

// Source creates a StreamDef for a source stream of type T.
func Source[T any](topic string, opts ...pubsub.StreamOption) StreamCreationOptions {
	return StreamCreationOptions{
		streamID: pubsub.MakeStreamID[T](topic),
		streamCreateFunc: func(repo *pubsub.StreamRepository) (pubsub.StreamID, error) {
			return pubsub.GetOrAddStreamOnRepository[T](repo, topic, append(opts, pubsub.WithAutoStart(true))...)
		},
	}
}

// From adds a stream to the query.
func (b *Builder) From(createFunc StreamCreationOptions) *Builder {
	if b.err != nil {
		return b
	}

	sid := createFunc.streamID
	if sid == pubsub.NilStreamID() {
		b.err = ErrNilStream
		return b
	}

	b.streams[sid] = createFunc.streamCreateFunc
	b.query.addStreams(sid)
	b.current = append(b.current, sid)
	return b
}

// AddInput adds a stream to the query. It is an alias for From.
func (b *Builder) AddInput(createFunc StreamCreationOptions) *Builder {
	return b.From(createFunc)
}

// Merge merges the state of another builder into this one.
func (b *Builder) Merge(b2 *Builder) *Builder {
	if b.err != nil {
		return b
	}
	if b2 == nil {
		return b
	}
	if b2.err != nil {
		b.err = b2.err
		return b
	}

	for id, s := range b2.streams {
		if _, ok := b.streams[id]; !ok {
			b.streams[id] = s
			b.query.addStreams(id)
		}
	}

	for id, op := range b2.operators {
		if _, ok := b.operators[id]; !ok {
			b.operators[id] = op
			b.query.addOperations(id)
		}
	}

	b.current = append(b.current, b2.current...)

	return b
}

func Operator[TOut any](operatorCreateFunc CreateOperatorFunc, opts ...pubsub.StreamOption) OperatorCreationOptions {
	outTopic := uuid.New().String()
	return OperatorCreationOptions{
		operatorCreateFunc: operatorCreateFunc,
		operatorID:         NewOperatorID(),

		outStreamCreateOptions: []StreamCreationOptions{{
			streamCreateFunc: func(repo *pubsub.StreamRepository) (pubsub.StreamID, error) {
				return pubsub.AddOrReplaceStreamOnRepository[TOut](repo, outTopic, opts...)
			},
			streamID: pubsub.MakeStreamID[TOut](outTopic),
		}},
	}
}

func CreateFanOutStream[TOut any](operatorCreateFunc CreateOperatorFunc, numOutputs int, opts ...pubsub.StreamOption) OperatorCreationOptions {
	outOptions := make([]StreamCreationOptions, numOutputs)
	for i := 0; i < numOutputs; i++ {
		outTopic := uuid.New().String()
		outOptions[i] = StreamCreationOptions{
			streamCreateFunc: func(repo *pubsub.StreamRepository) (pubsub.StreamID, error) {
				return pubsub.AddOrReplaceStreamOnRepository[TOut](repo, outTopic, opts...)
			},
			streamID: pubsub.MakeStreamID[TOut](outTopic),
		}
	}
	return OperatorCreationOptions{
		operatorCreateFunc:     operatorCreateFunc,
		outStreamCreateOptions: outOptions,
		operatorID:             NewOperatorID(),
	}
}

// Process adds an operator to the query
func (b *Builder) Process(operatorFunc OperatorCreationOptions) *Builder {
	if b.err != nil {
		return b
	}

	if len(b.current) == 0 {
		b.err = ErrEmptyInput
		return b
	}

	// Create output streams
	outputs := make([]pubsub.StreamID, 0, len(operatorFunc.outStreamCreateOptions))
	for _, outOpt := range operatorFunc.outStreamCreateOptions {
		outSid := outOpt.streamID
		if outSid == pubsub.NilStreamID() {
			b.err = ErrNilStream
			return b
		}

		b.streams[outSid] = outOpt.streamCreateFunc
		b.query.addStreams(outSid)
		outputs = append(outputs, outSid)
	}

	// Create operator
	inputs := b.current
	opID := operatorFunc.operatorID
	opF := func() (OperatorID, error) {
		return operatorFunc.operatorCreateFunc(inputs, outputs, opID)
	}

	b.operators[opID] = opF
	b.query.addOperations(opID)

	b.current = outputs
	return b
}

// Build constructs the TypedContinuousQuery.
func (b *Builder) Build(run bool) (ContinuousQuery, error) {
	var returnErr error

	if b.err != nil {
		return nil, b.err
	}

	if len(b.current) == 0 {
		return nil, ErrOutputUndefined
	}
	if len(b.current) > 1 {
		return nil, ErrAmbiguousOutput
	}
	b.query.out(b.current[0])

	// create all streams
	for _, streamF := range b.streams {
		_, err := streamF(b.query.repository())
		if err != nil {
			returnErr = errors.Join(returnErr, err)
		}
	}

	// create all operators
	for _, opf := range b.operators {
		_, err := opf()
		if err != nil {
			returnErr = errors.Join(returnErr, err)
		}
	}

	if returnErr != nil {
		b.query.close()

		return nil, returnErr
	}

	// start all streams and operators
	if run {
		err := b.query.Run()
		if err != nil {
			return nil, err
		}
	}

	return b.query, nil
}

// Builder 2:

// Deprecated: Use NewBuilder() and Builder.From() instead.
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

// Deprecated: Use NewBuilder() and Builder.Process() instead.
func Process[T any](
	operatorCreationFunc func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error),
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

		operatorEngine, err2 := operatorCreationFunc([]pubsub.StreamID{from.streamID}, []pubsub.StreamID{to}, NilOperatorID())
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

// Deprecated: Use NewBuilder() instead.
func Query[T any](
	fromF func(q ContinuousQuery) StreamWError,
	opts ...QueryOption,
) (q ContinuousQuery, err error) {

	q = newContinuousQuery[T](opts...)

	from := fromF(q)
	if from.error == nil {
		q.out(from.streamID)
	}

	if from.error != nil {
		q.close()
		return nil, from.error
	}

	return q, from.error
}

// Deprecated: Use NewBuilder() instead.
func OnStream[T any](stream StreamWError) StreamWError {
	return stream
}

// Deprecated: Use NewBuilder() instead.
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
