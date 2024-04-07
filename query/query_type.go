package query

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/buffer"
	"go-stream-processing/engine"
	"go-stream-processing/events"
	"go-stream-processing/streams"
	"go.uber.org/zap"
)

type QueryID uuid.UUID

func (q QueryID) String() string {
	return q.String()
}

func createStream[T any](eventTopic string, async bool) (streams.Stream[T], error) {
	var (
		stream streams.Stream[T]
		err    error
	)

	if existingStream, err := streams.GetStreamN[T](eventTopic); existingStream != nil {
		return existingStream, nil
	} else if errors.Is(err, streams.StreamNotFoundError()) {
		d := streams.NewStreamDescription(eventTopic, uuid.New(), async)

		if async {
			stream = streams.NewLocalSyncStream[T](d)
		} else {
			stream = streams.NewLocalAsyncStream[T](d)
		}

		err = streams.NewOrReplaceStream(stream)
		if err != nil {
			zap.S().Error("new stream cannot be created", zap.Error(err), zap.String("stream", eventTopic))
		}
	}

	return stream, err
}

func QueryOverSingleStreamSelection1[T any, TOut any](
	in string,
	operation func(engine.SingleStreamSelection1[T]) events.Event[TOut],
	out string) (streams.Stream[TOut], *QueryControl) {

	//TODO: errorhandling
	inputStream, _ := createStream[T](in, false)
	outputStream, _ := createStream[TOut](out, false)

	f, _ := createOperationOverSingleStreamSelection1(inputStream, operation, outputStream)

	queryControl := newQueryControl()
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)
	_ = QueryRepository().put(queryControl)

	queryControl.start()

	return outputStream, queryControl
}

func createOperationOverSingleStreamSelection1[T, TOut any](inputStream streams.Stream[T], operation func(engine.SingleStreamSelection1[T]) events.Event[TOut], outputStream streams.Stream[TOut]) (engine.OperatorControl, error) {
	inputSub := &engine.OperatorStreamSubscription[T]{
		Stream:      inputStream.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer[T](),
	}

	inStream := &engine.SingleStreamInput1[engine.SingleStreamSelection1[T], T]{
		Subscription: inputSub,
	}

	f := engine.NewOperator[engine.SingleStreamSelection1[T], TOut](operation, inStream, outputStream)

	err := OperatorRepository().Put(f)

	return f, err
}

func QueryOverSingleStreamSelectionN[T any, TOut any](in string,
	selection buffer.SelectionPolicy[T],
	op func(engine.SingleStreamSelectionN[T]) events.Event[TOut],
	out string) (streams.Stream[TOut], *QueryControl) {

	inputStream, _ := createStream[T](in, false)
	outputStream, _ := createStream[TOut](out, false)

	f := createOperatorOverSingleStreamSelectionN(inputStream, selection, op, outputStream)

	queryControl := newQueryControl()
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)
	_ = QueryRepository().put(queryControl)

	queryControl.start()

	return outputStream, queryControl
}

func createOperatorOverSingleStreamSelectionN[T any, TOut any](inputStream streams.Stream[T], selection buffer.SelectionPolicy[T], op func(engine.SingleStreamSelectionN[T]) events.Event[TOut], outputStream streams.Stream[TOut]) engine.OperatorControl {
	inputSub := &engine.OperatorStreamSubscription[T]{
		Stream:      inputStream.Subscribe(),
		InputBuffer: buffer.NewConsumableAsyncBuffer[T](selection),
	}

	inStream := &engine.SingleStreamInputN[engine.SingleStreamSelectionN[T], T]{
		Subscription: inputSub,
	}

	f := engine.NewOperator[engine.SingleStreamSelectionN[T], TOut](op, inStream, outputStream)

	_ = OperatorRepository().Put(f)
	return f
}

func QueryMultipleEventsOverSingleStreamSelection1[T any, TOut any](in string, op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], out string) (streams.Stream[TOut], *QueryControl) {

	inputStream, _ := createStream[T](in, false)
	outputStream, _ := createStream[TOut](out, false)

	f := createMultipleEventsOverSingleStreamSelection1(inputStream, op, outputStream)

	queryControl := newQueryControl()
	queryControl.addStreams(inputStream, outputStream)
	queryControl.addOperations(f)
	_ = QueryRepository().put(queryControl)

	queryControl.start()

	return outputStream, queryControl
}

func createMultipleEventsOverSingleStreamSelection1[T any, TOut any](inputStream streams.Stream[T], op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], outputStream streams.Stream[TOut]) engine.OperatorControl {
	inputSub := &engine.OperatorStreamSubscription[T]{
		Stream:      inputStream.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer[T](),
	}

	inStream := &engine.SingleStreamInput1[engine.SingleStreamSelection1[T], T]{
		Subscription: inputSub,
	}

	f := engine.NewOperatorN[engine.SingleStreamSelection1[T], TOut](op, inStream, outputStream)
	_ = OperatorRepository().Put(f)
	return f
}

func QueryOverDoubleStreamSelectionN[TIN1, TIN2, TOUT any](in1 string,
	selection1 buffer.SelectionPolicy[TIN1],
	in2 string,
	selection2 buffer.SelectionPolicy[TIN2],
	op func(n engine.DoubleInputSelectionN[TIN1, TIN2]) events.Event[TOUT],
	out string) (streams.Stream[TOUT], *QueryControl) {

	inputStream1, _ := createStream[TIN1](in1, false)
	inputStream2, _ := createStream[TIN2](in2, false)

	outputStream, _ := createStream[TOUT](out, false)

	inputSub1 := &engine.OperatorStreamSubscription[TIN1]{
		Stream:      inputStream1.Subscribe(),
		InputBuffer: buffer.NewConsumableAsyncBuffer[TIN1](selection1),
	}

	inputSub2 := &engine.OperatorStreamSubscription[TIN2]{
		Stream:      inputStream2.Subscribe(),
		InputBuffer: buffer.NewConsumableAsyncBuffer[TIN2](selection2),
	}

	inStream := &engine.DoubleStreamInputN[engine.DoubleInputSelectionN[TIN1, TIN2], TIN1, TIN2]{
		Subscription1: inputSub1,
		Subscription2: inputSub2,
	}

	f := engine.NewOperator[engine.DoubleInputSelectionN[TIN1, TIN2], TOUT](op, inStream, outputStream)

	queryControl := newQueryControl()
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)
	_ = QueryRepository().put(queryControl)

	queryControl.start()

	return outputStream, queryControl
}

func QueryOverDoubleStreamSelection1[TIN1, TIN2, TOUT any](in1 string,
	in2 string,
	op func(n engine.DoubleInputSelection1[TIN1, TIN2]) events.Event[TOUT],
	out string) (streams.Stream[TOUT], *QueryControl) {

	inputStream1, _ := createStream[TIN1](in1, false)
	inputStream2, _ := createStream[TIN2](in2, false)

	outputStream, _ := createStream[TOUT](out, false)

	inputSub1 := &engine.OperatorStreamSubscription[TIN1]{
		Stream:      inputStream1.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer[TIN1](),
	}

	inputSub2 := &engine.OperatorStreamSubscription[TIN2]{
		Stream:      inputStream2.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer[TIN2](),
	}

	inStream := &engine.DoubleStreamInput1[engine.DoubleInputSelection1[TIN1, TIN2], TIN1, TIN2]{
		Subscription1: inputSub1,
		Subscription2: inputSub2,
	}

	f := engine.NewOperator[engine.DoubleInputSelection1[TIN1, TIN2], TOUT](op, inStream, outputStream)

	queryControl := newQueryControl()
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)
	_ = QueryRepository().put(queryControl)

	queryControl.start()

	return outputStream, queryControl
}
