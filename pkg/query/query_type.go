package query

import (
	"errors"
	"github.com/google/uuid"
	buffer2 "go-stream-processing/internal/buffer"
	"go-stream-processing/internal/engine"
	"go-stream-processing/internal/events"
	streams2 "go-stream-processing/internal/streams"
	"go.uber.org/zap"
)

type QueryID uuid.UUID

func (q QueryID) String() string {
	return q.String()
}

func Close(c *QueryControl) {
	QueryRepository().remove(c.ID)
	c.stop()
}

func createStream[T any](eventTopic string, async bool) (streams2.Stream[T], error) {
	var (
		stream streams2.Stream[T]
		err    error
	)

	if existingStream, err := streams2.GetStreamN[T](eventTopic); existingStream != nil {
		return existingStream, nil
	} else if errors.Is(err, streams2.StreamNotFoundError()) {
		d := streams2.NewStreamDescription(eventTopic, uuid.New(), async)

		if async {
			stream = streams2.NewLocalSyncStream[T](d)
		} else {
			stream = streams2.NewLocalAsyncStream[T](d)
		}

		err = streams2.NewOrReplaceStream(stream)
		if err != nil {
			zap.S().Error("new stream cannot be created", zap.Error(err), zap.String("stream", eventTopic))
		}
	}

	return stream, err
}

func QueryOverSingleStreamSelection1[T any, TOut any](
	in string,
	operation func(engine.SingleStreamSelection1[T]) events.Event[TOut],
	out string) (streams2.Stream[TOut], *QueryControl) {

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

func createOperationOverSingleStreamSelection1[T, TOut any](inputStream streams2.Stream[T], operation func(engine.SingleStreamSelection1[T]) events.Event[TOut], outputStream streams2.Stream[TOut]) (engine.OperatorControl, error) {

	inStream := engine.NewSingleStreamInput1[T](inputStream)

	f := engine.NewOperator[engine.SingleStreamSelection1[T], TOut](operation, inStream, outputStream)

	err := OperatorRepository().Put(f)

	return f, err
}

func QueryOverSingleStreamSelectionN[T any, TOut any](in string,
	selection buffer2.SelectionPolicy[T],
	op func(engine.SingleStreamSelectionN[T]) events.Event[TOut],
	out string) (streams2.Stream[TOut], *QueryControl) {

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

func createOperatorOverSingleStreamSelectionN[T any, TOut any](inputStream streams2.Stream[T], selection buffer2.SelectionPolicy[T], op func(engine.SingleStreamSelectionN[T]) events.Event[TOut], outputStream streams2.Stream[TOut]) engine.OperatorControl {

	inStream := engine.NewSingleStreamInputN(inputStream, selection)

	f := engine.NewOperator[engine.SingleStreamSelectionN[T], TOut](op, inStream, outputStream)

	_ = OperatorRepository().Put(f)
	return f
}

func QueryMultipleEventsOverSingleStreamSelection1[T any, TOut any](in string, op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], out string) (streams2.Stream[TOut], *QueryControl) {

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

func createMultipleEventsOverSingleStreamSelection1[T any, TOut any](inputStream streams2.Stream[T], op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], outputStream streams2.Stream[TOut]) engine.OperatorControl {
	inStream := engine.NewSingleStreamInput1(inputStream)
	f := engine.NewOperatorN[engine.SingleStreamSelection1[T], TOut](op, inStream, outputStream)
	_ = OperatorRepository().Put(f)
	return f
}

func QueryOverDoubleStreamSelectionN[TIN1, TIN2, TOUT any](in1 string,
	selection1 buffer2.SelectionPolicy[TIN1],
	in2 string,
	selection2 buffer2.SelectionPolicy[TIN2],
	op func(n engine.DoubleInputSelectionN[TIN1, TIN2]) events.Event[TOUT],
	out string) (streams2.Stream[TOUT], *QueryControl) {

	inputStream1, _ := createStream[TIN1](in1, false)
	inputStream2, _ := createStream[TIN2](in2, false)

	outputStream, _ := createStream[TOUT](out, false)

	inputSub1 := &engine.OperatorStreamSubscription[TIN1]{
		Stream:      inputStream1.Subscribe(),
		InputBuffer: buffer2.NewConsumableAsyncBuffer[TIN1](selection1),
	}

	inputSub2 := &engine.OperatorStreamSubscription[TIN2]{
		Stream:      inputStream2.Subscribe(),
		InputBuffer: buffer2.NewConsumableAsyncBuffer[TIN2](selection2),
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
	out string) (streams2.Stream[TOUT], *QueryControl) {

	inputStream1, _ := createStream[TIN1](in1, false)
	inputStream2, _ := createStream[TIN2](in2, false)

	outputStream, _ := createStream[TOUT](out, false)

	inStream := engine.NewDoubleStreamInput1[TIN1, TIN2](inputStream1, inputStream2)

	f := engine.NewOperator[engine.DoubleInputSelection1[TIN1, TIN2], TOUT](op, inStream, outputStream)

	queryControl := newQueryControl()
	queryControl.addStreams(inputStream1, inputStream2, outputStream)
	queryControl.addOperations(f)
	_ = QueryRepository().put(queryControl)

	queryControl.start()

	return outputStream, queryControl
}
