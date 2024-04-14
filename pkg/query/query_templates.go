package query

import (
	"go-stream-processing/internal/engine"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
	"go-stream-processing/pkg/selection"
)

func TemplateQueryOverSingleStreamSelection1[T any, TOut any](
	in pubsub.StreamID,
	operation func(engine.SingleStreamSelection1[T]) events.Event[TOut],
	out pubsub.StreamID) (*ContinuousQuery, error) {

	inputStream, err := pubsub.GetOrCreateStream[T](in, false)
	if err != nil {
		return nil, err
	}
	outputStream, _ := pubsub.GetOrCreateStream[TOut](out, false)
	if err != nil {
		pubsub.TryRemoveStreams([]pubsub.StreamControl{inputStream})
		return nil, err
	}

	f, _ := createOperationOverSingleStreamSelection1(inputStream.ID(), operation, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)
	if err != nil {
		pubsub.TryRemoveStreams([]pubsub.StreamControl{inputStream, outputStream})
		engine.OperatorRepository().Remove([]engine.OperatorControl{f})
		return nil, err
	}

	return queryControl, nil
}

func createOperationOverSingleStreamSelection1[T, TOut any](inputStream pubsub.StreamID, operation func(engine.SingleStreamSelection1[T]) events.Event[TOut], outputStream pubsub.Stream[TOut]) (engine.OperatorControl, error) {

	inStream := engine.NewSingleStreamInput1[T](inputStream)

	f := engine.NewOperator[engine.SingleStreamSelection1[T], TOut](operation, inStream, outputStream)
	err := engine.OperatorRepository().Put(f)

	return f, err
}

func TemplateQueryOverSingleStreamSelectionN[T any, TOut any](
	in pubsub.StreamID,
	selection selection.Policy[T],
	op func(engine.SingleStreamSelectionN[T]) events.Event[TOut],
	out pubsub.StreamID) (*ContinuousQuery, error) {

	inputStream, err := pubsub.GetOrCreateStream[T](in, false)
	if err != nil {
		return nil, err
	}

	outputStream, err := pubsub.GetOrCreateStream[TOut](out, false)
	if err != nil {
		// cleanup
		pubsub.TryRemoveStreams([]pubsub.StreamControl{inputStream})
		return nil, err
	}

	f := createOperatorOverSingleStreamSelectionN(inputStream.ID(), selection, op, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)

	if err != nil {
		// cleanup
		pubsub.TryRemoveStreams([]pubsub.StreamControl{outputStream, inputStream})
		engine.OperatorRepository().Remove([]engine.OperatorControl{f})
		return nil, err
	}

	return queryControl, nil
}

func createOperatorOverSingleStreamSelectionN[T any, TOut any](inputStream pubsub.StreamID, selection selection.Policy[T], op func(engine.SingleStreamSelectionN[T]) events.Event[TOut], outputStream pubsub.Stream[TOut]) engine.OperatorControl {

	inStream := engine.NewSingleStreamInputN(inputStream, selection)

	f := engine.NewOperator[engine.SingleStreamSelectionN[T], TOut](op, inStream, outputStream)

	_ = engine.OperatorRepository().Put(f)
	return f
}

func TemplateQueryMultipleEventsOverSingleStreamSelection1[T any, TOut any](in pubsub.StreamID, op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], out pubsub.StreamID) (*ContinuousQuery, error) {

	inputStream, err := pubsub.GetOrCreateStream[T](in, false)
	if err != nil {
		return nil, err
	}
	outputStream, err := pubsub.GetOrCreateStream[TOut](out, false)
	if err != nil {
		pubsub.TryRemoveStreams([]pubsub.StreamControl{inputStream})
		return nil, err
	}

	f := createMultipleEventsOverSingleStreamSelection1(inputStream.ID(), op, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)

	if err != nil {
		pubsub.TryRemoveStreams([]pubsub.StreamControl{inputStream})
		engine.OperatorRepository().Remove([]engine.OperatorControl{f})
		return nil, err
	}

	return queryControl, nil
}

func createMultipleEventsOverSingleStreamSelection1[T any, TOut any](inputStream pubsub.StreamID, op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], outputStream pubsub.Stream[TOut]) engine.OperatorControl {
	inStream := engine.NewSingleStreamInput1[T](inputStream)
	f := engine.NewOperatorN[engine.SingleStreamSelection1[T], TOut](op, inStream, outputStream)
	_ = engine.OperatorRepository().Put(f)
	return f
}

func TemplateQueryOverDoubleStreamSelectionN[TIN1, TIN2, TOUT any](
	in1 pubsub.StreamID,
	selection1 selection.Policy[TIN1],
	in2 pubsub.StreamID,
	selection2 selection.Policy[TIN2],
	op func(n engine.DoubleInputSelectionN[TIN1, TIN2]) events.Event[TOUT],
	out pubsub.StreamID) (pubsub.Stream[TOUT], *ContinuousQuery) {

	inputStream1, _ := pubsub.GetOrCreateStream[TIN1](in1, false)
	inputStream2, _ := pubsub.GetOrCreateStream[TIN2](in2, false)

	outputStream, _ := pubsub.GetOrCreateStream[TOUT](out, false)

	inStream := engine.NewDoubleStreamInputN[TIN1, TIN2](inputStream1.ID(), selection1, inputStream2.ID(), selection2)

	f := engine.NewOperator[engine.DoubleInputSelectionN[TIN1, TIN2], TOUT](op, inStream, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)

	return outputStream, queryControl
}

func QueryOverDoubleStreamSelection1[TIN1, TIN2, TOUT any](
	in1 pubsub.StreamID,
	in2 pubsub.StreamID,
	op func(n engine.DoubleInputSelection1[TIN1, TIN2]) events.Event[TOUT],
	out pubsub.StreamID) (*ContinuousQuery, error) {

	inputStream1, _ := pubsub.GetOrCreateStream[TIN1](in1, false)
	inputStream2, _ := pubsub.GetOrCreateStream[TIN2](in2, false)

	outputStream, _ := pubsub.GetOrCreateStream[TOUT](out, false)

	inStream := engine.NewDoubleStreamInput1[TIN1, TIN2](inputStream1.ID(), inputStream2.ID())

	f := engine.NewOperator[engine.DoubleInputSelection1[TIN1, TIN2], TOUT](op, inStream, outputStream)

	queryControl := newQueryControl(outputStream.ID())
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)

	return queryControl, nil
}
