package query

import (
	"github.com/ottenwbe/go-streaming/internal/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

// Constraint to limit the type parameter to numeric types
type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// ContinuousBatchSum creates a query that sums numeric events over a window defined by the selection policy.
func ContinuousBatchSum[TEvent number](inEventType, outEventType string, selectionPolicy selection.PolicyDescription) (*ContinuousQuery, error) {

	batchSumF := func(input []events.Event[TEvent]) []TEvent {
		var result = TEvent(0)
		for _, event := range input {
			result += event.GetContent()
		}
		return []TEvent{result}
	}

	config := engine.NewOperatorDescription(
		engine.PIPELINE_OPERATOR,
		engine.WithOutput(outEventType),
		engine.WithInput(engine.InputDescription{
			Stream:      inEventType,
			InputPolicy: selectionPolicy,
		}),
	)

	op, err := engine.NewOperator[TEvent, TEvent](batchSumF, config)

	q := newQueryControl[TEvent](outEventType)
	q.addOperations(op)

	return q, err
}

// ContinuousBatchCount creates a query that counts events over a window defined by the selection policy.
func ContinuousBatchCount[TEvent any, TOut number](inEventType, outEventType string, selectionPolicy selection.PolicyDescription) (*ContinuousQuery, error) {

	batchCount := func(input []events.Event[TEvent]) []TOut {
		result := TOut(len(input))
		return []TOut{result}
	}

	config := engine.NewOperatorDescription(
		engine.PIPELINE_OPERATOR,
		engine.WithOutput(outEventType),
		engine.WithInput(engine.InputDescription{
			Stream:      inEventType,
			InputPolicy: selectionPolicy,
		}),
	)

	op, err := engine.NewOperator[TEvent, TEvent](batchCount, config)

	q := newQueryControl[TOut](outEventType)
	q.addOperations(op)

	return q, err
}

// ContinuousGreater creates a query that filters events greater than a specified value.
func ContinuousGreater[T number](inEventType, outEventType string, greaterThan T) (*ContinuousQuery, error) {

	greater := func(input events.Event[T]) bool {
		return input.GetContent() > greaterThan
	}

	config := engine.NewOperatorDescription(
		engine.FILTER_OPERATOR,
		engine.WithOutput(outEventType),
		engine.WithInput(engine.InputDescription{
			Stream: inEventType,
		}),
	)

	op, err := engine.NewOperator[T, T](greater, config)

	q := newQueryControl[T](outEventType)
	q.addOperations(op)

	return q, err
}

// ContinuousSmaller creates a query that filters events smaller than a specified value.
func ContinuousSmaller[T number](inEventType, outEventType string, than T) (*ContinuousQuery, error) {

	smaller := func(input events.Event[T]) bool {
		return input.GetContent() < than
	}

	config := engine.NewOperatorDescription(
		engine.FILTER_OPERATOR,
		engine.WithOutput(outEventType),
		engine.WithInput(engine.InputDescription{
			Stream: inEventType,
		}),
	)

	op, err := engine.NewOperator[T, T](smaller, config)

	q := newQueryControl[T](outEventType)
	q.addOperations(op)

	return q, err
}

// ContinuousConvert creates a query that converts events from one numeric type to another.
func ContinuousConvert[TIn, TOut number](inEventType, outEventType string) (*ContinuousQuery, error) {

	convert := func(input events.Event[TIn]) TOut {
		return TOut(input.GetContent())
	}

	config := engine.NewOperatorDescription(
		engine.MAP_OPERATOR,
		engine.WithOutput(outEventType),
		engine.WithInput(engine.InputDescription{
			Stream: inEventType,
		}),
	)

	op, err := engine.NewOperator[TIn, TOut](convert, config)

	q := newQueryControl[TOut](outEventType)
	q.addOperations(op)

	return q, err
}
