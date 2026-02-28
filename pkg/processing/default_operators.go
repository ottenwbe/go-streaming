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

// Constraint to limit the type parameter to numeric types
type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// BatchSum creates a query that sums numeric events over a window defined by the selection policy.
func BatchSum[TEvent number](policy events.PolicyDescription) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

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
func BatchCount[TEvent any, TOut number](policy events.PolicyDescription) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

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

		return NewOperator[TEvent, TEvent](batchCount, config, id)
	}
}

// Greater creates a query that filters events greater than a specified value.
func Greater[T number](greaterThan T) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

		greater := func(input events.Event[T]) bool {
			return input.GetContent() > greaterThan
		}

		config := MakeOperatorConfig(
			FILTER_OPERATOR,
			WithInput(MakeInputConfigs(in, events.PolicyDescription{})...),
			WithOutput(out...),
		)

		return NewOperator[T, T](greater, config, id)
	}
}

// Smaller creates a query that filters events smaller than a specified value.
func Smaller[T number](than T) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

		smaller := func(input events.Event[T]) bool {
			return input.GetContent() < than
		}

		config := MakeOperatorConfig(
			FILTER_OPERATOR,
			WithInput(MakeInputConfigs(in, events.PolicyDescription{})...),
			WithOutput(out...),
		)

		return NewOperator[T, T](smaller, config, id)
	}
}

// Convert creates a query that converts events from one numeric type to another.
func Convert[TIn, TOut number]() func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
		convert := func(input events.Event[TIn]) TOut {
			return TOut(input.GetContent())
		}

		config := MakeOperatorConfig(
			MAP_OPERATOR,
			WithInput(MakeInputConfigs(in, events.PolicyDescription{})...),
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
			WithInput(MakeInputConfigs(in, events.PolicyDescription{})...),
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
			WithInput(MakeInputConfigs(in, events.PolicyDescription{})...),
			WithOutput(out...),
		)
		return NewOperator[TIn, TOut](mapper, config, id)
	}
}

// Join creates a join operator that performs an inner join on two streams of maps.
// It joins on the provided key and merges the two event maps.
func Join(
	key string,
	policy events.PolicyDescription,
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
	policy events.PolicyDescription,
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
