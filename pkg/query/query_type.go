package query

import (
	buffer2 "go-stream-processing/internal/buffer"
	"go-stream-processing/internal/engine"
	"go-stream-processing/internal/events"
	"go-stream-processing/internal/pubsub"
)

func TemplateQueryOverSingleStreamSelection1[T any, TOut any](
	in string,
	operation func(engine.SingleStreamSelection1[T]) events.Event[TOut],
	out string) (*QueryControl, error) {

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

	queryControl := newQueryControl(outputStream)
	queryControl.addStreams(inputStream)
	queryControl.addOperations(f)
	err = QueryRepository().put(queryControl)
	if err != nil {
		pubsub.TryRemoveStreams([]pubsub.StreamControl{inputStream, outputStream})
		engine.OperatorRepository().Remove([]engine.OperatorControl{f})
		return nil, err
	}

	queryControl.startEverything()

	return queryControl, nil
}

func createOperationOverSingleStreamSelection1[T, TOut any](inputStream pubsub.StreamID, operation func(engine.SingleStreamSelection1[T]) events.Event[TOut], outputStream pubsub.Stream[TOut]) (engine.OperatorControl, error) {

	inStream := engine.NewSingleStreamInput1[T](inputStream)

	f := engine.NewOperator[engine.SingleStreamSelection1[T], TOut](operation, inStream, outputStream)
	err := engine.OperatorRepository().Put(f)

	return f, err
}

func TemplateQueryOverSingleStreamSelectionN[T any, TOut any](in string,
	selection buffer2.SelectionPolicy[T],
	op func(engine.SingleStreamSelectionN[T]) events.Event[TOut],
	out string) (*QueryControl, error) {

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

	queryControl := newQueryControl(outputStream)
	queryControl.addStreams(inputStream)
	queryControl.addOperations(f)
	err = QueryRepository().put(queryControl)
	if err != nil {
		// cleanup
		pubsub.TryRemoveStreams([]pubsub.StreamControl{outputStream, inputStream})
		engine.OperatorRepository().Remove([]engine.OperatorControl{f})
		return nil, err
	}

	return queryControl, nil
}

func createOperatorOverSingleStreamSelectionN[T any, TOut any](inputStream pubsub.StreamID, selection buffer2.SelectionPolicy[T], op func(engine.SingleStreamSelectionN[T]) events.Event[TOut], outputStream pubsub.Stream[TOut]) engine.OperatorControl {

	inStream := engine.NewSingleStreamInputN(inputStream, selection)

	f := engine.NewOperator[engine.SingleStreamSelectionN[T], TOut](op, inStream, outputStream)

	_ = engine.OperatorRepository().Put(f)
	return f
}

func TemplateQueryMultipleEventsOverSingleStreamSelection1[T any, TOut any](in string, op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], out string) (*QueryControl, error) {

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

	queryControl := newQueryControl(outputStream)
	queryControl.addStreams(inputStream)
	queryControl.addOperations(f)
	err = QueryRepository().put(queryControl)
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

func TemplateQueryOverDoubleStreamSelectionN[TIN1, TIN2, TOUT any](in1 string,
	selection1 buffer2.SelectionPolicy[TIN1],
	in2 string,
	selection2 buffer2.SelectionPolicy[TIN2],
	op func(n engine.DoubleInputSelectionN[TIN1, TIN2]) events.Event[TOUT],
	out string) (pubsub.Stream[TOUT], *QueryControl) {

	inputStream1, _ := pubsub.GetOrCreateStream[TIN1](in1, false)
	inputStream2, _ := pubsub.GetOrCreateStream[TIN2](in2, false)

	outputStream, _ := pubsub.GetOrCreateStream[TOUT](out, false)

	inStream := engine.NewDoubleStreamInputN[TIN1, TIN2](inputStream1.ID(), selection1, inputStream2.ID(), selection2)

	f := engine.NewOperator[engine.DoubleInputSelectionN[TIN1, TIN2], TOUT](op, inStream, outputStream)

	queryControl := newQueryControl(outputStream)
	queryControl.addStreams(inputStream1, inputStream2)
	queryControl.addOperations(f)
	_ = QueryRepository().put(queryControl)

	return outputStream, queryControl
}

func QueryOverDoubleStreamSelection1[TIN1, TIN2, TOUT any](in1 string,
	in2 string,
	op func(n engine.DoubleInputSelection1[TIN1, TIN2]) events.Event[TOUT],
	out string) (*QueryControl, error) {

	inputStream1, _ := pubsub.GetOrCreateStream[TIN1](in1, false)
	inputStream2, _ := pubsub.GetOrCreateStream[TIN2](in2, false)

	outputStream, _ := pubsub.GetOrCreateStream[TOUT](out, false)

	inStream := engine.NewDoubleStreamInput1[TIN1, TIN2](inputStream1.ID(), inputStream2.ID())

	f := engine.NewOperator[engine.DoubleInputSelection1[TIN1, TIN2], TOUT](op, inStream, outputStream)

	queryControl := newQueryControl(outputStream)
	queryControl.addStreams(inputStream1, inputStream2)
	queryControl.addOperations(f)
	_ = QueryRepository().put(queryControl)

	return queryControl, nil
}
