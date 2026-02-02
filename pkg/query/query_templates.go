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

	inputStreamID, err := pubsub.AddOrReplaceStreamFromDescription[T](pubsub.MakeStreamDescription[T](in, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}
	outputStreamID, err := pubsub.AddOrReplaceStreamFromDescription[TOut](pubsub.MakeStreamDescription[TOut](out, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}

	f := createOperationOverSingleStreamSelection1(inputStreamID, operation, outputStreamID)

	queryControl := newQueryControl(outputStreamID)
	queryControl.addStreams(inputStreamID, outputStreamID)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createOperationOverSingleStreamSelection1[T, TOut any](inputStream pubsub.StreamID, operation func(engine.SingleStreamSelection1[T]) events.Event[TOut], outputStream pubsub.StreamID) engine.OperatorControl {

	inStream := engine.NewSingleStreamInput1[T](inputStream)

	f := engine.NewOperator[engine.SingleStreamSelection1[T], TOut](operation, inStream, outputStream)

	return f
}

// TemplateQueryOverSingleStreamSelectionN creates a query that processes a selection of multiple events from one input stream.
func TemplateQueryOverSingleStreamSelectionN[T any, TOut any](
	in string,
	selection selection.Policy[T],
	op func(engine.SingleStreamSelectionN[T]) events.Event[TOut],
	out string) (*ContinuousQuery, error) {

	inputStreamID, err := pubsub.AddOrReplaceStreamFromDescription[T](pubsub.MakeStreamDescription[T](in, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}
	outputStreamID, err := pubsub.AddOrReplaceStreamFromDescription[TOut](pubsub.MakeStreamDescription[TOut](out, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}

	f := createOperatorOverSingleStreamSelectionN(inputStreamID, selection, op, outputStreamID)

	queryControl := newQueryControl(outputStreamID)
	queryControl.addStreams(inputStreamID, outputStreamID)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createOperatorOverSingleStreamSelectionN[T any, TOut any](inputStream pubsub.StreamID, selection selection.Policy[T], op func(engine.SingleStreamSelectionN[T]) events.Event[TOut], outputStream pubsub.StreamID) engine.OperatorControl {
	inStream := engine.NewSingleStreamInputN(inputStream, selection)
	f := engine.NewOperator[engine.SingleStreamSelectionN[T], TOut](op, inStream, outputStream)
	return f
}

// TemplateQueryMultipleEventsOverSingleStreamSelection1 creates a query that maps a single input event to multiple output events.
func TemplateQueryMultipleEventsOverSingleStreamSelection1[T any, TOut any](
	in string,
	op func(engine.SingleStreamSelection1[T]) []events.Event[TOut],
	out string) (*ContinuousQuery, error) {

	inputStreamID, err := pubsub.AddOrReplaceStreamFromDescription[T](pubsub.MakeStreamDescription[T](in, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}
	outputStreamID, err := pubsub.AddOrReplaceStreamFromDescription[TOut](pubsub.MakeStreamDescription[TOut](out, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}

	f := createMultipleEventsOverSingleStreamSelection1(inputStreamID, op, outputStreamID)

	queryControl := newQueryControl(outputStreamID)
	queryControl.addStreams(inputStreamID, outputStreamID)
	queryControl.addOperations(f)

	return queryControl, nil
}

func createMultipleEventsOverSingleStreamSelection1[T any, TOut any](inputStream pubsub.StreamID, op func(engine.SingleStreamSelection1[T]) []events.Event[TOut], outputStream pubsub.StreamID) engine.OperatorControl {
	inStream := engine.NewSingleStreamInput1[T](inputStream)
	f := engine.NewOperatorN[engine.SingleStreamSelection1[T], TOut](op, inStream, outputStream)
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

	inputStream1ID, err := pubsub.AddOrReplaceStreamFromDescription[TIN1](pubsub.MakeStreamDescription[TIN1](in1, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}
	inputStream2ID, err := pubsub.AddOrReplaceStreamFromDescription[TIN2](pubsub.MakeStreamDescription[TIN2](in2, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}
	outputStreamID, err := pubsub.AddOrReplaceStreamFromDescription[TOUT](pubsub.MakeStreamDescription[TOUT](out, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}

	inStream := engine.NewDoubleStreamInputN[TIN1, TIN2](inputStream1ID, selection1, inputStream2ID, selection2)
	f := engine.NewOperator[engine.DoubleInputSelectionN[TIN1, TIN2], TOUT](op, inStream, outputStreamID)

	queryControl := newQueryControl(outputStreamID)
	queryControl.addStreams(inputStream1ID, inputStream2ID, outputStreamID)
	queryControl.addOperations(f)

	return queryControl, nil
}

// TemplateQueryOverDoubleStreamSelection1 creates a query that processes single events from two input streams.
func TemplateQueryOverDoubleStreamSelection1[TIN1, TIN2, TOUT any](
	in1 string,
	in2 string,
	op func(n engine.DoubleInputSelection1[TIN1, TIN2]) events.Event[TOUT],
	out string) (*ContinuousQuery, error) {

	inputStream1ID, err := pubsub.AddOrReplaceStreamFromDescription[TIN1](pubsub.MakeStreamDescription[TIN1](in1, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}
	inputStream2ID, err := pubsub.AddOrReplaceStreamFromDescription[TIN2](pubsub.MakeStreamDescription[TIN2](in2, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}
	outputStreamID, err := pubsub.AddOrReplaceStreamFromDescription[TOUT](pubsub.MakeStreamDescription[TOUT](out, pubsub.WithSingleFanIn(true)))
	if err != nil {
		return nil, err
	}

	inStream := engine.NewDoubleStreamInput1[TIN1, TIN2](inputStream1ID, inputStream2ID)
	f := engine.NewOperator[engine.DoubleInputSelection1[TIN1, TIN2], TOUT](op, inStream, outputStreamID)

	queryControl := newQueryControl(outputStreamID)
	queryControl.addStreams(inputStream1ID, inputStream2ID, outputStreamID)
	queryControl.addOperations(f)

	return queryControl, nil
}
