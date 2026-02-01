package query

import (
	"github.com/ottenwbe/go-streaming/internal/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	pubsub "github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

// TemplateQueryOverSingleStreamSelection1 creates a query that processes single events from one input stream.
func TemplateQueryOverSingleStreamSelection1[T any, TOut any](
	in string,
	operation func(engine.SingleStreamSelection1[T]) events.Event[TOut],
	out string) (*ContinuousQuery, error) {

	inputStream := pubsub.NewStream[T](in, false, true)
	outputStream := pubsub.NewStream[TOut](out, false, true)

	f := createOperationOverSingleStreamSelection1(inputStream.ID(), operation, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createOperationOverSingleStreamSelection1[T, TOut any](inputStream pubsub.StreamID, operation func(engine.SingleStreamSelection1[T]) events.Event[TOut], outputStream pubsub.Stream) engine.OperatorControl {

	inStream := engine.NewSingleStreamInput1[T](inputStream)

	f := engine.NewOperator[engine.SingleStreamSelection1[T], TOut](operation, inStream, outputStream.ID())

	return f
}

// TemplateQueryOverSingleStreamSelectionN creates a query that processes a selection of multiple events from one input stream.
func TemplateQueryOverSingleStreamSelectionN[T any, TOut any](
	in string,
	selection selection.Policy[T],
	op func(engine.SingleStreamSelectionN[T]) events.Event[TOut],
	out string) (*ContinuousQuery, error) {

	inputStream := pubsub.NewStream[T](in, false, true)
	outputStream := pubsub.NewStream[TOut](out, false, true)

	f := createOperatorOverSingleStreamSelectionN(inputStream.ID(), selection, op, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createOperatorOverSingleStreamSelectionN[T any, TOut any](inputStream pubsub.StreamID, selection selection.Policy[T], op func(engine.SingleStreamSelectionN[T]) events.Event[TOut], outputStream pubsub.Stream) engine.OperatorControl {
	inStream := engine.NewSingleStreamInputN(inputStream, selection)
	f := engine.NewOperator[engine.SingleStreamSelectionN[T], TOut](op, inStream, outputStream.ID())
	return f
}

// TemplateQueryMultipleEventsOverSingleStreamSelection1 creates a query that maps a single input event to multiple output events.
func TemplateQueryMultipleEventsOverSingleStreamSelection1[T any, TOut any](
	in string,
	op func(engine.SingleStreamSelection1[T]) []events.Event[TOut],
	out string) (*ContinuousQuery, error) {

	inputStream := pubsub.NewStream[T](in, false, true)
	outputStream := pubsub.NewStream[TOut](out, false, true)

	f := createMultipleEventsOverSingleStreamSelection1(inputStream.ID(), op, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createMultipleEventsOverSingleStreamSelection1[T any, TOut any](inputStream pubsub.StreamID, op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], outputStream pubsub.Stream) engine.OperatorControl {
	inStream := engine.NewSingleStreamInput1[T](inputStream)
	f := engine.NewOperatorN[engine.SingleStreamSelection1[T], TOut](op, inStream, outputStream.ID())
	return f
}

// TemplateQueryOverDoubleStreamSelectionN creates a query that processes selections from two input streams.
func TemplateQueryOverDoubleStreamSelectionN[TIN1, TIN2, TOUT any](
	in1 string,
	selection1 selection.Policy[TIN1],
	in2 string,
	selection2 selection.Policy[TIN2],
	op func(n engine.DoubleInputSelectionN[TIN1, TIN2]) events.Event[TOUT],
	out string) (*ContinuousQuery, error) {

	inputStream1 := pubsub.NewStream[TIN1](in1, false, true)
	inputStream2 := pubsub.NewStream[TIN2](in2, false, true)

	outputStream := pubsub.NewStream[TOUT](out, false, true)

	inStream := engine.NewDoubleStreamInputN[TIN1, TIN2](inputStream1.ID(), selection1, inputStream2.ID(), selection2)
	f := engine.NewOperator[engine.DoubleInputSelectionN[TIN1, TIN2], TOUT](op, inStream, outputStream.ID())

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}

// TemplateQueryOverDoubleStreamSelection1 creates a query that processes single events from two input streams.
func TemplateQueryOverDoubleStreamSelection1[TIN1, TIN2, TOUT any](
	in1 string,
	in2 string,
	op func(n engine.DoubleInputSelection1[TIN1, TIN2]) events.Event[TOUT],
	out string) (*ContinuousQuery, error) {

	inputStream1 := pubsub.NewStream[TIN1](in1, false, true)
	inputStream2 := pubsub.NewStream[TIN2](in2, false, true)

	outputStream := pubsub.NewStream[TOUT](out, false, true)

	inStream := engine.NewDoubleStreamInput1[TIN1, TIN2](inputStream1.ID(), inputStream2.ID())
	f := engine.NewOperator[engine.DoubleInputSelection1[TIN1, TIN2], TOUT](op, inStream, outputStream.ID())

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}
