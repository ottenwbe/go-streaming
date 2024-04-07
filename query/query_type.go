package engine

import (
	"github.com/google/uuid"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
	"go-stream-processing/streams"
	"go.uber.org/zap"
)

type QueryID uuid.UUID

func (q QueryID) String() string {
	return q.String()
}

func createStream[T any](eventTopic string, async bool) (streams.Stream[T], error) {
	var stream streams.Stream[T]

	d := streams.NewStreamDescription(eventTopic, uuid.New(), async)

	if async {
		stream = streams.NewLocalSyncStream[T](d)
	} else {
		stream = streams.NewLocalAsyncStream[T](d)
	}

	err := streams.NewOrReplaceStream(stream)
	if err != nil {
		zap.S().Error("new stream cannot be created", zap.Error(err), zap.String("stream", eventTopic))
	}

	return stream, err
}

func QueryOverSingleStreamSelection1[T any, TOut any](
	in string,
	operation func(SingleStreamSelection1[T]) events.Event[TOut],
	out string) streams.Stream[TOut] {

	//TODO: errorhandling
	inputStream, _ := createStream[T](in, false)
	outputStream, _ := createStream[TOut](out, false)

	inputSub, f, _ := createOperationOverSingleStreamSelection1(inputStream, operation, outputStream)

	inputSub.Run()
	f.Start()

	return outputStream
}

func createOperationOverSingleStreamSelection1[T, TOut any](inputStream streams.Stream[T], operation func(SingleStreamSelection1[T]) events.Event[TOut], outputStream streams.Stream[TOut]) (*OperatorStreamSubscription[T], OperatorControl, error) {
	inputSub := &OperatorStreamSubscription[T]{
		Stream:      inputStream.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer[T](),
	}

	inStream := &SingleStreamInput1[SingleStreamSelection1[T], T]{
		Subscription: inputSub,
	}

	f := NewOperator[SingleStreamSelection1[T], TOut](operation, inStream, outputStream)

	err := OperatorRepository().Put(f)

	return inputSub, f, err
}

func QueryOverSingleStreamSelectionN[T any, TOut any](in string,
	selection buffer.SelectionPolicy[T],
	op func(SingleStreamSelectionN[T]) events.Event[TOut],
	out string) streams.Stream[TOut] {

	inputStream, _ := createStream[T](in, false)
	outputStream, _ := createStream[TOut](out, false)

	inputSub := &OperatorStreamSubscription[T]{
		Stream:      inputStream.Subscribe(),
		InputBuffer: buffer.NewConsumableAsyncBuffer[T](selection),
	}

	inStream := &SingleStreamInputN[SingleStreamSelectionN[T], T]{
		Subscription: inputSub,
	}

	f := NewOperator[SingleStreamSelectionN[T], TOut](op, inStream, outputStream)

	_ = OperatorRepository().Put(f)

	inputSub.Run()
	f.Start()

	return outputStream
}

func QueryMultipleEventsOverSingleStreamSelection1[T any, TOut any](in string,
	op func(SingleStreamSelection1[T]) []events.Event[TOut],
	out string) streams.Stream[TOut] {

	inputStream, _ := createStream[T](in, false)
	outputStream, _ := createStream[TOut](out, false)

	inputSub := &OperatorStreamSubscription[T]{
		Stream:      inputStream.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer[T](),
	}

	inStream := &SingleStreamInput1[SingleStreamSelection1[T], T]{
		Subscription: inputSub,
	}

	f := NewOperatorN[SingleStreamSelection1[T], TOut](op, inStream, outputStream)
	_ = OperatorRepository().Put(f)

	inputSub.Run()
	f.Start()

	return outputStream
}

func QueryOverDoubleStreamSelectionN[TIN1, TIN2, TOUT any](in1 string,
	selection1 buffer.SelectionPolicy[TIN1],
	in2 string,
	selection2 buffer.SelectionPolicy[TIN2],
	op func(n DoubleInputSelectionN[TIN1, TIN2]) events.Event[TOUT],
	out string) {

	inputStream1, _ := streams.SubscribeN[TIN1](in1)
	inputStream2, _ := streams.SubscribeN[TIN2](in2)

	outputStream, _ := streams.GetStreamN[TOUT](out)

	inputSub1 := &OperatorStreamSubscription[TIN1]{
		Stream:      inputStream1,
		InputBuffer: buffer.NewConsumableAsyncBuffer[TIN1](selection1),
	}

	inputSub2 := &OperatorStreamSubscription[TIN2]{
		Stream:      inputStream2,
		InputBuffer: buffer.NewConsumableAsyncBuffer[TIN2](selection2),
	}

	inStream := &DoubleStreamInputN[DoubleInputSelectionN[TIN1, TIN2], TIN1, TIN2]{
		Subscription1: inputSub1,
		Subscription2: inputSub2,
	}

	f := NewOperator[DoubleInputSelectionN[TIN1, TIN2], TOUT](op, inStream, outputStream)

	inputSub1.Run()
	inputSub2.Run()
	f.Start()
}
