package query

import (
	"go-stream-processing/internal/engine"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/selection"
)

// Constraint to limit the type parameter to numeric types
type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

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

func ContinuousBatchCount[TEvent any, TOut number](inEventType, outEvenType string, selectionPolicy selection.Policy[TEvent]) (*ContinuousQuery, error) {

	batchCount := func(input engine.SingleStreamSelectionN[TEvent]) events.Event[TOut] {
		result := TOut(len(input))
		return events.NewNumericEvent[TOut](result)
	}

	return TemplateQueryOverSingleStreamSelectionN[TEvent, TOut](inEventType, selectionPolicy, batchCount, outEvenType)
}

func ContinuousGreater[T number](inEventType, outEvenType string, greaterThan T) (*ContinuousQuery, error) {

	greater := func(input engine.SingleStreamSelection1[T]) []events.Event[T] {
		if input.GetContent() > greaterThan {
			return []events.Event[T]{input}
		} else {
			return []events.Event[T]{}
		}
	}

	return TemplateQueryMultipleEventsOverSingleStreamSelection1[T, T](inEventType, greater, outEvenType)
}

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

func ContinuousAdd[T number](inEventType1, inEventType2, outEventType string) (*ContinuousQuery, error) {

	add := func(input engine.DoubleInputSelection1[T, T]) events.Event[T] {
		result := input.Input1.GetContent() + input.Input2.GetContent()
		return events.NewEvent[T](result)
	}

	return TemplateQueryOverDoubleStreamSelection1[T, T, T](inEventType1, inEventType2, add, outEventType)
}

func ContinuousConvert[TIn, TOut number](inEventType, outEventType string) (*ContinuousQuery, error) {

	convert := func(input engine.SingleStreamSelection1[TIn]) events.Event[TOut] {
		return events.NewEvent[TOut](TOut(input.GetContent()))
	}

	return TemplateQueryOverSingleStreamSelection1[TIn, TOut](inEventType, convert, outEventType)
}
