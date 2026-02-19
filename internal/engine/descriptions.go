package engine

import (
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

const (
	PIPELINE_OPERATOR = "PIPELINE_OPERATOR"
	FILTER_OPERATOR   = "FILTER_OPERATOR"
	MAP_OPERATOR      = "MAP_OPERATOR"
)

type InputDescription struct {
	Stream      string                      `yaml:"stream" json:"stream"`
	InputPolicy selection.PolicyDescription `yaml:"policy" json:"policy"`
}

type OperatorDescription struct {
	Type    string             `yaml:"type" json:"type"`
	ID      OperatorID         `yaml:"id" json:"id"`
	Inputs  []InputDescription `yaml:"inputs" json:"inputs"`
	Outputs []string           `yaml:"outputs" json:"outputs"`
}

type OperatorOption func(*OperatorDescription)

func WithInput(inputs ...InputDescription) OperatorOption {
	return func(o *OperatorDescription) {
		o.Inputs = append(o.Inputs, inputs...)
	}
}

func WithOutput(topics ...string) OperatorOption {
	return func(o *OperatorDescription) {
		o.Outputs = append(o.Outputs, topics...)
	}
}

func NewOperatorDescription(operatorType string, opts ...OperatorOption) *OperatorDescription {
	op := &OperatorDescription{
		Type:    operatorType,
		ID:      NewOperatorID(),
		Inputs:  []InputDescription{},
		Outputs: []string{},
	}
	for _, opt := range opts {
		opt(op)
	}
	return op
}
