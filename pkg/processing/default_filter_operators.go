package processing

import (
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

// Filter creates a query that filters events based on a provided predicate.
func Filter[T any](predicate func(events.Event[T]) bool) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
		config := MakeOperatorConfig(
			FILTER_OPERATOR,
			WithInput(MakeInputConfigs(in, events.SelectionPolicyConfig{})...),
			WithOutput(out...),
		)
		return NewOperator[T, T](predicate, config, id)
	}
}

// Greater creates a query that filters events greater than a specified value.
func Greater[T events.NumericConstraint](greaterThan T) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	greater := func(input events.Event[T]) bool {
		return input.GetContent() > greaterThan
	}
	return Filter(greater)
}

// Smaller creates a query that filters events smaller than a specified value.
func Smaller[T events.NumericConstraint](than T) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	smaller := func(input events.Event[T]) bool {
		return input.GetContent() < than
	}
	return Filter(smaller)
}

// Even creates a query that filters events that are even numbers.
func Even[T events.NumericConstraint]() func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	even := func(input events.Event[T]) bool {
		return int(input.GetContent())%2 == 0
	}
	return Filter(even)
}

// Odd creates a query that filters events that are odd numbers.
func Odd[T events.NumericConstraint]() func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	odd := func(input events.Event[T]) bool {
		return int(input.GetContent())%2 != 0
	}
	return Filter(odd)
}

// Limit creates a query that allows only the first n events to pass.
func Limit[T any](n int) func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id OperatorID) (OperatorID, error) {
		count := 0
		limit := func(input events.Event[T]) bool {
			if count < n {
				count++
				return true
			}
			return false
		}
		return Filter(limit)(in, out, id)
	}
}
