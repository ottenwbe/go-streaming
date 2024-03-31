package engine

import (
	"go-stream-processing/events"
)

// Constraint to limit the type parameter to numeric types
type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func BatchSum[T number](input SingleStreamSelectionN[T]) events.Event[T] {
	var result = T(0)
	for _, event := range input {
		result += event.GetContent()
	}
	return events.NewEvent[T](result)
}

func BatchCount[T number, TOut number](input SingleStreamSelectionN[T]) events.Event[TOut] {
	result := TOut(len(input))
	return events.NewEvent[TOut](result)
}

func Add[T number](input DoubleInputSelection[T, T]) events.Event[T] {
	result := input.input1[0].GetContent() + input.input2[0].GetContent()
	return events.NewEvent[T](result)
}

func Convert[TIn, TOut number](input events.Event[TIn]) events.Event[TOut] {
	return events.NewEvent[TOut](TOut(input.GetContent()))
}
