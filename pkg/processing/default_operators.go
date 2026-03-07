package processing

import (
	"errors"
	"maps"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var (
	ErrJoinRequiresTwoInputs     = errors.New("Join operator requires exactly two input streams")
	ErrLeftJoinRequiresTwoInputs = errors.New("LeftJoin operator requires exactly two input streams")
)

// BatchSum creates a query that sums numeric events over a window defined by the selection policy.
func BatchSum[TEvent events.NumericConstraint](policy events.SelectionPolicyConfig) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

		batchSumF := func(input []events.Event[TEvent]) []TEvent {
			var result = TEvent(0)
			for _, event := range input {
				result += event.GetContent()
			}
			return []TEvent{result}
		}

		config := MakeOperatorConfig(
			PIPELINE_OPERATOR,
			WithInput(MakeInputConfigs(in, policy)...),
			WithOutput(out...),
		)

		op, err := NewOperator[TEvent, TEvent](batchSumF, config, id)

		return op, err
	}
}

// BatchCount creates a query that counts events over a window defined by the selection policy.
func BatchCount[TEvent any, TOut events.NumericConstraint](policy events.SelectionPolicyConfig) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

		batchCount := func(input []events.Event[TEvent]) []TOut {
			result := TOut(len(input))
			return []TOut{result}
		}

		config := MakeOperatorConfig(
			PIPELINE_OPERATOR,
			WithInput(MakeInputConfigs(in, policy)...),
			WithOutput(out...),
		)

		return NewOperator[TEvent, TOut](batchCount, config, id)
	}
}

// Convert creates a query that converts events from one numeric type to another.
func Convert[TIn, TOut events.NumericConstraint]() func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
		convert := func(input events.Event[TIn]) TOut {
			return TOut(input.GetContent())
		}

		config := MakeOperatorConfig(
			MAP_OPERATOR,
			WithInput(MakeInputConfigs(in, events.SelectionPolicyConfig{})...),
			WithOutput(out...),
		)

		return NewOperator[TIn, TOut](convert, config, id)
	}
}

// SelectFromMap creates an operator that selects a value from a map by key and forwards it.
// The input stream must contain events of type `map[string]any`. The output will be of type `any`.
// If the key is not found, the operator forwards an event with nil content.
func SelectFromMap(key string) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

		mapper := func(event events.Event[map[string]any]) any {
			if content := event.GetContent(); content != nil {
				if value, ok := content[key]; ok {
					return value
				}
			}
			return nil
		}

		config := MakeOperatorConfig(
			MAP_OPERATOR,
			WithInput(MakeInputConfigs(in, events.SelectionPolicyConfig{})...),
			WithOutput(out...),
		)

		return NewOperator[map[string]any, any](mapper, config, id)
	}
}

// Map creates a query that maps events from one type to another using a provided mapper function.
func Map[TIn, TOut any](mapper func(events.Event[TIn]) TOut) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
		config := MakeOperatorConfig(
			MAP_OPERATOR,
			WithInput(MakeInputConfigs(in, events.SelectionPolicyConfig{})...),
			WithOutput(out...),
		)
		return NewOperator[TIn, TOut](mapper, config, id)
	}
}

// Join creates a join operator that performs an inner join on two streams of maps.
// It joins on the provided key and merges the two event maps.
func Join(
	key string,
	policy events.SelectionPolicyConfig,
) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
		if len(in) != 2 {
			return NilOperatorID(), ErrJoinRequiresTwoInputs
		}

		joinFunc := func(inputs map[int][]events.Event[map[string]any]) []map[string]any {
			stream0Events := inputs[0]
			stream1Events := inputs[1]

			// matching events from the right stream
			stream1Map := make(map[any][]events.Event[map[string]any])
			for _, event := range stream1Events {
				if val, ok := event.GetContent()[key]; ok {
					stream1Map[val] = append(stream1Map[val], event)
				}
			}

			var results []map[string]any
			// Iterate through the left stream and find matches in the right stream's map
			for _, event0 := range stream0Events {
				if val0, ok := event0.GetContent()[key]; ok {
					if matchingEvents, found := stream1Map[val0]; found {
						for _, event1 := range matchingEvents {
							merged := make(map[string]any)
							maps.Copy(merged, event0.GetContent())
							maps.Copy(merged, event1.GetContent())
							results = append(results, merged)
						}
					}
				}
			}
			return results
		}

		config := MakeOperatorConfig(
			FANIN_OPERATOR,
			WithInput(MakeInputConfigs(in, policy)...),
			WithOutput(out...),
		)

		return NewOperator[map[string]any, map[string]any](joinFunc, config, id)
	}
}

// LeftJoin creates a join operator that performs a left outer join on two streams of maps.
func LeftJoin(
	key string,
	policy events.SelectionPolicyConfig,
) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
		if len(in) != 2 {
			return NilOperatorID(), ErrLeftJoinRequiresTwoInputs
		}

		joinFunc := func(inputs map[int][]events.Event[map[string]any]) []map[string]any {
			stream0Events := inputs[0]
			stream1Events := inputs[1]

			stream1Map := make(map[any][]events.Event[map[string]any])
			for _, event := range stream1Events {
				if val, ok := event.GetContent()[key]; ok {
					stream1Map[val] = append(stream1Map[val], event)
				}
			}

			var results []map[string]any
			for _, event0 := range stream0Events {
				val0, ok0 := event0.GetContent()[key]
				matched := false
				if ok0 {
					if matchingEvents, ok := stream1Map[val0]; ok {
						for _, event1 := range matchingEvents {
							merged := make(map[string]any)
							maps.Copy(merged, event0.GetContent())
							maps.Copy(merged, event1.GetContent())
							results = append(results, merged)
							matched = true
						}
					}
				}
				if !matched {
					merged := make(map[string]any)
					maps.Copy(merged, event0.GetContent())
					results = append(results, merged)
				}
			}
			return results
		}

		config := MakeOperatorConfig(
			FANIN_OPERATOR,
			WithInput(MakeInputConfigs(in, policy)...),
			WithOutput(out...),
		)

		return NewOperator[map[string]any, map[string]any](joinFunc, config, id)
	}
}

// FlatMap creates a query that maps one input event to zero or more output events.
func FlatMap[TIn, TOut any](mapper func(events.Event[TIn]) []TOut) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

		batchMapper := func(input []events.Event[TIn]) []TOut {
			var result []TOut
			for _, event := range input {
				result = append(result, mapper(event)...)
			}
			return result
		}

		policy := events.SelectionPolicyConfig{Type: events.SelectNext, Active: true}

		config := MakeOperatorConfig(
			PIPELINE_OPERATOR,
			WithInput(MakeInputConfigs(in, policy)...),
			WithOutput(out...),
		)

		return NewOperator[TIn, TOut](batchMapper, config, id)
	}
}

// Observe creates an operator that executes a side-effect for each event but passes it through unchanged.
func Observe[T any](callback func(events.Event[T])) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

		mapper := func(event events.Event[T]) T {
			callback(event)
			return event.GetContent()
		}

		config := MakeOperatorConfig(
			MAP_OPERATOR,
			WithInput(MakeInputConfigs(in, events.SelectionPolicyConfig{})...),
			WithOutput(out...),
		)

		return NewOperator[T, T](mapper, config, id)
	}
}

// Tokenize creates a query that splits a string into individual words.
// func Tokenize() func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
// 	return FlatMap[string, string](func(e events.Event[string]) []string {
// 		return strings.Fields(e.GetContent())
// 	})
// }
