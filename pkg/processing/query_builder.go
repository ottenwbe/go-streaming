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

//NOTE: first draft

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

func (b *Builder) Join(b2 *Builder) *Builder {
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
	b.query.out(b.current[0])

	for _, streamf := range b.streams {
		_, err := streamf(b.query.repository())
		if err != nil {
			returnErr = errors.Join(returnErr, err)
		}
	}

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

	if run {
		err := b.query.Run()
		if err != nil {
			return nil, err
		}
	}

	return b.query, nil
}
