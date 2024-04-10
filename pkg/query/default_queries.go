package query

import (
	"go-stream-processing/internal/buffer"
	"go-stream-processing/internal/engine"
	"go-stream-processing/internal/events"
)

// Constraint to limit the type parameter to numeric types
type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func ContinuousBatchSum[TEvent number](inEventType string, outEvenType string, selectionPolicy buffer.SelectionPolicy[TEvent]) (*QueryControl, error) {

	batchSumF := func(input engine.SingleStreamSelectionN[TEvent]) events.Event[TEvent] {
		var result = TEvent(0)
		for _, event := range input {
			result += event.GetContent()
		}
		return events.NewNumericEvent[TEvent](result)
	}

	return QueryOverSingleStreamSelectionN[TEvent, TEvent](inEventType, selectionPolicy, batchSumF, outEvenType)
}

func ContinuousBatchCount[TEvent any, TOut number](inEventType string, outEvenType string, selectionPolicy buffer.SelectionPolicy[TEvent]) (*QueryControl, error) {

	batchCount := func(input engine.SingleStreamSelectionN[TEvent]) events.Event[TOut] {
		result := TOut(len(input))
		return events.NewNumericEvent[TOut](result)
	}

	return QueryOverSingleStreamSelectionN[TEvent, TOut](inEventType, selectionPolicy, batchCount, outEvenType)
}

func ContinuousGreater[T number](inEventType string, greaterThan T, outEvenType string) (*QueryControl, error) {

	greater := func(input engine.SingleStreamSelection1[T]) []events.Event[T] {
		if input.GetContent() > greaterThan {
			return []events.Event[T]{input}
		} else {
			return []events.Event[T]{}
		}
	}

	return QueryMultipleEventsOverSingleStreamSelection1[T, T](inEventType, greater, outEvenType)
}

func ContinuousSmaller[T number](inEventType, outEvenType string, than T) (*QueryControl, error) {

	smaller := func(input engine.SingleStreamSelection1[T]) []events.Event[T] {
		if input.GetContent() < than {
			return []events.Event[T]{input}
		} else {
			return []events.Event[T]{}
		}

	}
	return QueryMultipleEventsOverSingleStreamSelection1[T, T](inEventType, smaller, outEvenType)
}

func ContinuousAdd[T number](inEventType1, inEventType2 string, outEventType string) (*QueryControl, error) {

	add := func(input engine.DoubleInputSelection1[T, T]) events.Event[T] {
		result := input.Input1.GetContent() + input.Input2.GetContent()
		return events.NewEvent[T](result)
	}

	return QueryOverDoubleStreamSelection1[T, T, T](inEventType1, inEventType2, add, outEventType)
}

func ContinuousConvert[TIn, TOut number](inEventType string, outEventType string) (*QueryControl, error) {

	convert := func(input engine.SingleStreamSelection1[TIn]) events.Event[TOut] {
		return events.NewEvent[TOut](TOut(input.GetContent()))
	}

	return QueryOverSingleStreamSelection1[TIn, TOut](inEventType, convert, outEventType)
}
