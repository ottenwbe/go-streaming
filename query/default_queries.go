package engine

import (
	"go-stream-processing/buffer"
	"go-stream-processing/events"
	"go-stream-processing/streams"
)

// Constraint to limit the type parameter to numeric types
type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func QueryBatchSum[TEvent number](inEventType string, outEvenType string, selectionPolicy buffer.SelectionPolicy[TEvent]) streams.Stream[TEvent] {

	batchSumF := func(input SingleStreamSelectionN[TEvent]) events.Event[TEvent] {
		var result = TEvent(0)
		for _, event := range input {
			result += event.GetContent()
		}
		return events.NewNumericEvent[TEvent](result)
	}

	return QueryOverSingleStreamSelectionN[TEvent, TEvent](inEventType, selectionPolicy, batchSumF, outEvenType)
}

func QueryBatchCount[TEvent any, TOut number](inEventType string, outEvenType string, selectionPolicy buffer.SelectionPolicy[TEvent]) streams.Stream[TOut] {

	batchCount := func(input SingleStreamSelectionN[TEvent]) events.Event[TOut] {
		result := TOut(len(input))
		return events.NewNumericEvent[TOut](result)
	}

	return QueryOverSingleStreamSelectionN[TEvent, TOut](inEventType, selectionPolicy, batchCount, outEvenType)
}

func QueryGreater[T number](inEventType string, greaterThan T, outEvenType string) streams.Stream[T] {

	greater := func(input SingleStreamSelection1[T]) []events.Event[T] {
		if input.GetContent() > greaterThan {
			return []events.Event[T]{input}
		} else {
			return []events.Event[T]{}
		}
	}

	return QueryMultipleEventsOverSingleStreamSelection1[T, T](inEventType, greater, outEvenType)
}

func Smaller[T number](v T) func(input SingleStreamSelection1[T]) []events.Event[T] {
	return func(input SingleStreamSelection1[T]) []events.Event[T] {
		if input.GetContent() < v {
			return []events.Event[T]{input}
		} else {
			return []events.Event[T]{}
		}
	}
}

func Add[T number](input DoubleInputSelection1[T, T]) events.Event[T] {
	result := input.input1.GetContent() + input.input2.GetContent()
	return events.NewEvent[T](result)
}

func QueryConvert[TIn, TOut number](inEventType string, outEventType string) streams.Stream[TOut] {

	convert := func(input SingleStreamSelection1[TIn]) events.Event[TOut] {
		return events.NewEvent[TOut](TOut(input.GetContent()))
	}

	return QueryOverSingleStreamSelection1[TIn, TOut](inEventType, convert, outEventType)
}
