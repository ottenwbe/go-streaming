package engine

import (
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

const (
	PIPELINE_OPERATOR = "PIPELINE_OPERATOR"
	FILTER_OPERATOR   = "FILTER_OPERATOR"
	MAP_OPERATOR      = "MAP_OPERATOR"
)

type InputDescription struct {
	Stream      pubsub.StreamID             `yaml:"stream" json:"stream"`
	InputPolicy selection.PolicyDescription `yaml:"policy" json:"policy"`
}

type OperatorDescription struct {
	AutoStart bool               `yaml:"auto_start" json:"auto_start"`
	Type      string             `yaml:"type" json:"type"`
	ID        OperatorID         `yaml:"id" json:"id"`
	Inputs    []InputDescription `yaml:"inputs" json:"inputs"`
	Outputs   []pubsub.StreamID  `yaml:"outputs" json:"outputs"`
}

type OperatorOption func(*OperatorDescription)

func WithInput(inputs ...InputDescription) OperatorOption {
	return func(o *OperatorDescription) {
		o.Inputs = append(o.Inputs, inputs...)
	}
}

func WithAutoStart(auto bool) OperatorOption {
	return func(o *OperatorDescription) {
		o.AutoStart = auto
	}
}

func WithOutput(topics ...pubsub.StreamID) OperatorOption {
	return func(o *OperatorDescription) {
		o.Outputs = append(o.Outputs, topics...)
	}
}

func InputDescriptions(in []pubsub.StreamID, policy selection.PolicyDescription) []InputDescription {
	res := make([]InputDescription, len(in))

	for i, id := range in {
		res[i] = InputDescription{
			Stream:      id,
			InputPolicy: policy,
		}
	}

	return res
}

func NewOperatorDescription(operatorType string, opts ...OperatorOption) *OperatorDescription {
	op := &OperatorDescription{
		Type:    operatorType,
		ID:      NewOperatorID(),
		Inputs:  []InputDescription{},
		Outputs: []pubsub.StreamID{},
	}
	for _, opt := range opts {
		opt(op)
	}
	return op
}
