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
func ContinuousBatchSum[TEvent number](inEventType, outEvenType string, selectionPolicy selection.Policy[TEvent]) (*ContinuousQuery, error) {

	batchSumF := func(input engine.SingleStreamSelectionN[TEvent]) events.Event[TEvent] {
		var result = TEvent(0)
		for _, event := range input {
			result += event.GetContent()
		}
		return events.NewNumericEvent[TEvent](result)
	}

	return TemplateQueryOverSingleStreamSelectionN[TEvent, TEvent](inEventType, selectionPolicy, batchSumF, outEvenType)
}

// ContinuousBatchCount creates a query that counts events over a window defined by the selection policy.
func ContinuousBatchCount[TEvent any, TOut number](inEventType, outEvenType string, selectionPolicy selection.Policy[TEvent]) (*ContinuousQuery, error) {

	batchCount := func(input engine.SingleStreamSelectionN[TEvent]) events.Event[TOut] {
		result := TOut(len(input))
		return events.NewNumericEvent[TOut](result)
	}

	return TemplateQueryOverSingleStreamSelectionN[TEvent, TOut](inEventType, selectionPolicy, batchCount, outEvenType)
}

// ContinuousGreater creates a query that filters events greater than a specified value.
func ContinuousGreater[T number](inEventType, outEvenType string, greaterThan T) (*ContinuousQuery, error) {

	greater := func(input engine.SingleStreamSelection1[T]) []events.Event[T] {
		if input.GetContent() > greaterThan {
			return []events.Event[T]{input}
		}

		return []events.Event[T]{}
	}

	return TemplateQueryMultipleEventsOverSingleStreamSelection1[T, T](inEventType, greater, outEvenType)
}

// ContinuousSmaller creates a query that filters events smaller than a specified value.
func ContinuousSmaller[T number](inEventType, outEvenType string, than T) (*ContinuousQuery, error) {

	smaller := func(input engine.SingleStreamSelection1[T]) []events.Event[T] {
		if input.GetContent() < than {
			return []events.Event[T]{input}
		} else {
			return []events.Event[T]{}
		}

	}
	return TemplateQueryMultipleEventsOverSingleStreamSelection1[T, T](inEventType, smaller, outEvenType)
}

// ContinuousAdd creates a query that adds values from two input streams.
func ContinuousAdd[T number](inEventType1, inEventType2, outEventType string) (*ContinuousQuery, error) {

	add := func(input engine.DoubleInputSelection1[T, T]) events.Event[T] {
		result := input.Input1.GetContent() + input.Input2.GetContent()
		return events.NewEvent[T](result)
	}

	return TemplateQueryOverDoubleStreamSelection1[T, T, T](inEventType1, inEventType2, add, outEventType)
}

// ContinuousConvert creates a query that converts events from one numeric type to another.
func ContinuousConvert[TIn, TOut number](inEventType, outEventType string) (*ContinuousQuery, error) {

	convert := func(input engine.SingleStreamSelection1[TIn]) events.Event[TOut] {
		return events.NewEvent[TOut](TOut(input.GetContent()))
	}

	return TemplateQueryOverSingleStreamSelection1[TIn, TOut](inEventType, convert, outEventType)
}
