package processing

import (
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

const (
	PIPELINE_OPERATOR = "PIPELINE_OPERATOR"
	FILTER_OPERATOR   = "FILTER_OPERATOR"
	MAP_OPERATOR      = "MAP_OPERATOR"
	FANOUT_OPERATOR   = "FANOUT_OPERATOR"
)

type InputConfig struct {
	Stream      pubsub.StreamID          `yaml:"stream" json:"stream"`
	InputPolicy events.PolicyDescription `yaml:"policy" json:"policy"`
}

type OperatorConfig struct {
	AutoStart bool              `yaml:"auto_start" json:"auto_start"`
	Type      string            `yaml:"type" json:"type"`
	ID        OperatorID        `yaml:"id" json:"id"`
	Inputs    []InputConfig     `yaml:"inputs" json:"inputs"`
	Outputs   []pubsub.StreamID `yaml:"outputs" json:"outputs"`
}

type OperatorOption func(*OperatorConfig)

func WithInput(inputs ...InputConfig) OperatorOption {
	return func(o *OperatorConfig) {
		o.Inputs = append(o.Inputs, inputs...)
	}
}

func WithAutoStart(auto bool) OperatorOption {
	return func(o *OperatorConfig) {
		o.AutoStart = auto
	}
}

func WithOutput(topics ...pubsub.StreamID) OperatorOption {
	return func(o *OperatorConfig) {
		o.Outputs = append(o.Outputs, topics...)
	}
}

func MakeInputConfigs(in []pubsub.StreamID, policy events.PolicyDescription) []InputConfig {
	res := make([]InputConfig, len(in))

	for i, id := range in {
		res[i] = InputConfig{
			Stream:      id,
			InputPolicy: policy,
		}
	}

	return res
}

func MakeOperatorConfig(operatorType string, opts ...OperatorOption) OperatorConfig {
	op := OperatorConfig{
		Type:    operatorType,
		ID:      NewOperatorID(),
		Inputs:  []InputConfig{},
		Outputs: []pubsub.StreamID{},
	}
	for _, opt := range opts {
		opt(&op)
	}
	return op
}
