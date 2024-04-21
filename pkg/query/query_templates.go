package query

import (
	"go-stream-processing/internal/engine"
	"go-stream-processing/pkg/events"
	pubsub "go-stream-processing/pkg/pubsub"
	"go-stream-processing/pkg/selection"
)

func TemplateQueryOverSingleStreamSelection1[T any, TOut any](
	in string,
	operation func(engine.SingleStreamSelection1[T]) events.Event[TOut],
	out string) (*ContinuousQuery, error) {

	inputStream := pubsub.NewStream[T](in, false)
	outputStream := pubsub.NewStream[TOut](out, false)

	f := createOperationOverSingleStreamSelection1(inputStream.ID(), operation, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createOperationOverSingleStreamSelection1[T, TOut any](inputStream pubsub.StreamID, operation func(engine.SingleStreamSelection1[T]) events.Event[TOut], outputStream pubsub.Stream[TOut]) engine.OperatorControl {

	inStream := engine.NewSingleStreamInput1[T](inputStream)

	f := engine.NewOperator[engine.SingleStreamSelection1[T], TOut](operation, inStream, outputStream.ID())

	return f
}

func TemplateQueryOverSingleStreamSelectionN[T any, TOut any](
	in string,
	selection selection.Policy[T],
	op func(engine.SingleStreamSelectionN[T]) events.Event[TOut],
	out string) (*ContinuousQuery, error) {

	inputStream := pubsub.NewStream[T](in, false)
	outputStream := pubsub.NewStream[TOut](out, false)

	f := createOperatorOverSingleStreamSelectionN(inputStream.ID(), selection, op, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createOperatorOverSingleStreamSelectionN[T any, TOut any](inputStream pubsub.StreamID, selection selection.Policy[T], op func(engine.SingleStreamSelectionN[T]) events.Event[TOut], outputStream pubsub.Stream[TOut]) engine.OperatorControl {
	inStream := engine.NewSingleStreamInputN(inputStream, selection)
	f := engine.NewOperator[engine.SingleStreamSelectionN[T], TOut](op, inStream, outputStream.ID())
	return f
}

func TemplateQueryMultipleEventsOverSingleStreamSelection1[T any, TOut any](
	in string,
	op func(engine.SingleStreamSelection1[T]) []events.Event[TOut],
	out string) (*ContinuousQuery, error) {

	inputStream := pubsub.NewStream[T](in, false)
	outputStream := pubsub.NewStream[TOut](out, false)

	f := createMultipleEventsOverSingleStreamSelection1(inputStream.ID(), op, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createMultipleEventsOverSingleStreamSelection1[T any, TOut any](inputStream pubsub.StreamID, op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], outputStream pubsub.Stream[TOut]) engine.OperatorControl {
	inStream := engine.NewSingleStreamInput1[T](inputStream)
	f := engine.NewOperatorN[engine.SingleStreamSelection1[T], TOut](op, inStream, outputStream.ID())
	return f
}

func TemplateQueryOverDoubleStreamSelectionN[TIN1, TIN2, TOUT any](
	in1 string,
	selection1 selection.Policy[TIN1],
	in2 string,
	selection2 selection.Policy[TIN2],
	op func(n engine.DoubleInputSelectionN[TIN1, TIN2]) events.Event[TOUT],
	out string) (pubsub.Stream[TOUT], *ContinuousQuery) {

	inputStream1 := pubsub.NewStream[TIN1](in1, false)
	inputStream2 := pubsub.NewStream[TIN2](in2, false)

	outputStream := pubsub.NewStream[TOUT](out, false)

	inStream := engine.NewDoubleStreamInputN[TIN1, TIN2](inputStream1.ID(), selection1, inputStream2.ID(), selection2)
	f := engine.NewOperator[engine.DoubleInputSelectionN[TIN1, TIN2], TOUT](op, inStream, outputStream.ID())

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)

	return outputStream, queryControl
}

func TemplateQueryOverDoubleStreamSelection1[TIN1, TIN2, TOUT any](
	in1 string,
	in2 string,
	op func(n engine.DoubleInputSelection1[TIN1, TIN2]) events.Event[TOUT],
	out string) (*ContinuousQuery, error) {

	inputStream1 := pubsub.NewStream[TIN1](in1, false)
	inputStream2 := pubsub.NewStream[TIN2](in2, false)

	outputStream := pubsub.NewStream[TOUT](out, false)

	inStream := engine.NewDoubleStreamInput1[TIN1, TIN2](inputStream1.ID(), inputStream2.ID())
	f := engine.NewOperator[engine.DoubleInputSelection1[TIN1, TIN2], TOUT](op, inStream, outputStream.ID())

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}
