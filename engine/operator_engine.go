package engine

import (
	"github.com/google/uuid"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
	"go-stream-processing/streams"
	"go.uber.org/zap"
)

type (
	Consumable[T any] interface {
		Consume() T
	}
	OperatorStreamSubscription[TSub any] struct {
		Stream      *streams.StreamReceiver[TSub]
		InputBuffer buffer.Buffer[TSub]
	}

	SingleStreamSelectionN[TIN any]                                       []events.Event[TIN]
	SingleStreamOperation[TIN, TOUT any]                                  func(events []events.Event[TIN]) []events.Event[TOUT]
	SingleStreamInputN[TRes SingleStreamSelectionN[TStream], TStream any] struct {
		Subscription *OperatorStreamSubscription[TStream]
	}
	SingleStreamInput1[TRes events.Event[TStream], TStream any] struct {
		Subscription *OperatorStreamSubscription[TStream]
	}

	DoubleInputSelection[TIN1, TIN2 any] struct {
		input1 []events.Event[TIN1]
		input2 []events.Event[TIN2]
	}
	DoubleStreamOperation[TIN1, TIN2, TOUT any]                                                  func(events DoubleInputSelection[TIN1, TIN2]) []events.Event[TOUT]
	DoubleStreamInput[TRes DoubleInputSelection[TStream1, TStream2], TStream1 any, TStream2 any] struct {
		Subscription1 *OperatorStreamSubscription[TStream1]
		Subscription2 *OperatorStreamSubscription[TStream2]
	}
)

func (s *SingleStreamInputN[TRes, TStream]) Consume() TRes {
	return s.Subscription.InputBuffer.GetAndConsumeNextEvents()
}

func (s *SingleStreamInput1[TRes, TStream]) Consume() TRes {
	return s.Subscription.InputBuffer.GetAndRemoveNextEvent().(TRes)
}

func (s *DoubleStreamInput[TRes, TStream1, TStream2]) Consume() TRes {
	return TRes{
		input1: s.Subscription1.InputBuffer.GetAndConsumeNextEvents(),
		input2: s.Subscription2.InputBuffer.GetAndConsumeNextEvents(),
	}
}

func (o *OperatorStreamSubscription[T]) Run() {
	go func() {
		for {
			event, more := <-o.Stream.Notify
			if more {
				o.InputBuffer.AddEvent(event)
			} else {
				return
			}
		}
	}()
}

func (o *OperatorStreamSubscription[T]) Consume() []events.Event[T] {
	return o.InputBuffer.GetAndConsumeNextEvents()
}

type Operator[TIN any, TOUT any] struct {
	id uuid.UUID
	f  func(in TIN) events.Event[TOUT]

	active bool

	Input  Consumable[TIN]
	Output streams.Stream[TOUT]
}

func NewOperator[TIn, Tout any](f func(TIn) events.Event[Tout], in Consumable[TIn], out streams.Stream[Tout]) *Operator[TIn, Tout] {
	return &Operator[TIn, Tout]{
		id:     uuid.New(),
		f:      f,
		Input:  in,
		Output: out,
		active: false,
	}
}

func (op *Operator[TIn, Tout]) Stop() {
	op.active = false
}

func (op *Operator[TIn, Tout]) Start() {

	if !op.active {
		op.active = true

		go func() {
			var (
				inputEvents TIn
				publishErr  error
			)
			for op.active {
				inputEvents = op.Input.Consume()

				resultEvent := op.f(inputEvents)

				if publishErr = op.Output.Publish(resultEvent); publishErr != nil {
					zap.S().Error("could not publish event in operator",
						zap.Error(publishErr),
						zap.String("operator", op.id.String()),
					)
				}
			}
		}()
	}
}
