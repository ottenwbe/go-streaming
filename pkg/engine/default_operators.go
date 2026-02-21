package engine

import (
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

// Constraint to limit the type parameter to numeric types
type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// ContinuousBatchSum creates a query that sums numeric events over a window defined by the selection policy.
func ContinuousBatchSum[TEvent number](policy selection.PolicyDescription) func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {

		batchSumF := func(input []events.Event[TEvent]) []TEvent {
			var result = TEvent(0)
			for _, event := range input {
				result += event.GetContent()
			}
			return []TEvent{result}
		}

		config := NewOperatorDescription(
			PIPELINE_OPERATOR,
		)

		op, err := NewOperator[TEvent, TEvent](batchSumF, config)

		return op, err
	}
}

// ContinuousBatchCount creates a query that counts events over a window defined by the selection policy.
func ContinuousBatchCount[TEvent any, TOut number](policy selection.PolicyDescription) func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {

		batchCount := func(input []events.Event[TEvent]) []TOut {
			result := TOut(len(input))
			return []TOut{result}
		}

		config := NewOperatorDescription(
			PIPELINE_OPERATOR,
			WithInput(InputDescriptions(in, policy)...),
			WithOutput(out...),
		)

		return NewOperator[TEvent, TEvent](batchCount, config)
	}
}

// ContinuousGreater creates a query that filters events greater than a specified value.
func ContinuousGreater[T number](greaterThan T) func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {

		greater := func(input events.Event[T]) bool {
			return input.GetContent() > greaterThan
		}

		config := NewOperatorDescription(
			FILTER_OPERATOR,
			WithInput(InputDescriptions(in, selection.PolicyDescription{})...),
			WithOutput(out...),
		)

		return NewOperator[T, T](greater, config)
	}
}

// ContinuousSmaller creates a query that filters events smaller than a specified value.
func ContinuousSmaller[T number](than T) func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {

		smaller := func(input events.Event[T]) bool {
			return input.GetContent() < than
		}

		config := NewOperatorDescription(
			FILTER_OPERATOR,
			WithInput(InputDescriptions(in, selection.PolicyDescription{})...),
			WithOutput(out...),
		)

		return NewOperator[T, T](smaller, config)
	}
}

// ContinuousConvert creates a query that converts events from one numeric type to another.
func ContinuousConvert[TIn, TOut number]() func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {

	return func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {
		convert := func(input events.Event[TIn]) TOut {
			return TOut(input.GetContent())
		}

		config := NewOperatorDescription(
			MAP_OPERATOR,
			WithInput(InputDescriptions(in, selection.PolicyDescription{})...),
			WithOutput(out...),
		)

		return NewOperator[TIn, TOut](convert, config)
	}
}

// ContinuousFanOut creates a query that broadcasts events to multiple output streams.
func ContinuousFanOut[T any]() func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID) (OperatorID, error) {
		config := NewOperatorDescription(
			FANOUT_OPERATOR,
			WithInput(InputDescriptions(in, selection.PolicyDescription{})...),
			WithOutput(out...),
		)
		return NewOperator[T, T](nil, config)
	}
}
