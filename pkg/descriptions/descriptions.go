package descriptions

import (
	"go-stream-processing/internal/engine"
	"go-stream-processing/pkg/pubsub"
	"reflect"
)

type Engine int

const (
	SingleStreamIn1Out1 Engine = iota
	SingleStreamInNOut1
)

type SelectionDescription struct {
	Stream    pubsub.StreamID
	InputType reflect.Type
}

type OperatorDescription struct {
	ID     engine.OperatorID
	Inputs []SelectionDescription
	Output pubsub.StreamID
	Engine Engine
}

type QueryDescription struct {
	Operators []OperatorDescription
	Streams   map[pubsub.StreamID]pubsub.StreamDescription
	Output    pubsub.StreamID
}

func NewQueryDescription() *QueryDescription {
	return &QueryDescription{
		Operators: make([]OperatorDescription, 0),
		Streams:   make(map[pubsub.StreamID]pubsub.StreamDescription),
		Output:    pubsub.NilStreamID(),
	}
}
