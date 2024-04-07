package engine

import (
	"github.com/google/uuid"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
	"go-stream-processing/streams"
	"go.uber.org/zap"
)

type (
	OperatorID uuid.UUID

	Consumable[T any] interface {
		Consume() T
		Run()
	}
	OperatorStreamSubscription[TSub any] struct {
		Stream      *streams.StreamReceiver[TSub]
		InputBuffer buffer.Buffer[TSub]
	}

	SingleStreamSelection1[TIN any]                                       events.Event[TIN]
	SingleStreamSelectionN[TIN any]                                       []events.Event[TIN]
	SingleStreamInputN[TRes SingleStreamSelectionN[TStream], TStream any] struct {
		Subscription *OperatorStreamSubscription[TStream]
	}
	SingleStreamInput1[TRes events.Event[TStream], TStream any] struct {
		Subscription *OperatorStreamSubscription[TStream]
	}

	DoubleInputSelectionN[TIN1, TIN2 any] struct {
		input1 []events.Event[TIN1]
		input2 []events.Event[TIN2]
	}
	DoubleInputSelection1[TIN1, TIN2 any] struct {
		Input1 events.Event[TIN1]
		Input2 events.Event[TIN2]
	}
	DoubleStreamInputN[TRes DoubleInputSelectionN[TStream1, TStream2], TStream1 any, TStream2 any] struct {
		Subscription1 *OperatorStreamSubscription[TStream1]
		Subscription2 *OperatorStreamSubscription[TStream2]
	}
	DoubleStreamInput1[TRes DoubleInputSelection1[TStream1, TStream2], TStream1 any, TStream2 any] struct {
		Subscription1 *OperatorStreamSubscription[TStream1]
		Subscription2 *OperatorStreamSubscription[TStream2]
	}
)

func (o OperatorID) String() string {
	return o.String()
}

func (s *SingleStreamInputN[TRes, TStream]) Consume() TRes {
	return s.Subscription.InputBuffer.GetAndConsumeNextEvents()
}

func (s *SingleStreamInputN[TRes, TStream]) Run() {
	s.Subscription.Run()
}

func (s *SingleStreamInput1[TRes, TStream]) Consume() TRes {
	return s.Subscription.InputBuffer.GetAndRemoveNextEvent().(TRes)
}

func (s *SingleStreamInput1[TRes, TStream]) Run() {
	s.Subscription.Run()
}

func (s *DoubleStreamInputN[TRes, TStream1, TStream2]) Consume() TRes {
	return TRes{
		input1: s.Subscription1.InputBuffer.GetAndConsumeNextEvents(),
		input2: s.Subscription2.InputBuffer.GetAndConsumeNextEvents(),
	}
}

func (s *DoubleStreamInputN[TRes, TStream1, TStream2]) Run() {
	s.Subscription1.Run()
	s.Subscription2.Run()
}

func (d *DoubleStreamInput1[TRes, TStream1, TStream2]) Consume() TRes {
	return TRes{
		Input1: d.Subscription1.InputBuffer.GetAndRemoveNextEvent(),
		Input2: d.Subscription2.InputBuffer.GetAndRemoveNextEvent(),
	}
}

func (d *DoubleStreamInput1[TRes, TStream1, TStream2]) Run() {
	d.Subscription1.Run()
	d.Subscription2.Run()
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

type OperatorControl interface {
	ID() OperatorID
	Start()
	Stop()
}

type Operator1[TIN any, TOUT any] struct {
	id OperatorID
	f  func(in TIN) events.Event[TOUT]

	active bool

	Input  Consumable[TIN]
	Output streams.Stream[TOUT]
}

type OperatorN[TIN any, TOUT any] struct {
	id OperatorID
	f  func(in TIN) []events.Event[TOUT]

	active bool

	Input  Consumable[TIN]
	Output streams.Stream[TOUT]
}

func (op *Operator1[TIn, Tout]) Stop() {
	op.active = false
}

func (op *Operator1[TIN, TOUT]) ID() OperatorID {
	return op.id
}

func (op *Operator1[TIn, Tout]) Start() {

	if !op.active {
		op.active = true

		go func() {
			var (
				inputEvents TIn
				publishErr  error
			)

			op.Input.Run()

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

func (op *OperatorN[TIN, TOUT]) Stop() {
	op.active = false
}

func (op *OperatorN[TIN, TOUT]) ID() OperatorID {
	return op.id
}

func (op *OperatorN[TIN, TOUT]) Start() {
	if !op.active {
		op.active = true

		go func() {
			var (
				inputEvents TIN
				publishErr  error
			)

			op.Input.Run()

			for op.active {
				inputEvents = op.Input.Consume()

				resultEvents := op.f(inputEvents)

				for _, resultEvent := range resultEvents {
					if publishErr = op.Output.Publish(resultEvent); publishErr != nil {
						zap.S().Error("could not publish event in operator",
							zap.Error(publishErr),
							zap.String("operator", op.id.String()),
						)
					}
				}
			}
			zap.S().Debug("operator stopped", zap.String("operator", op.id.String()))
		}()
	}
}

func NewOperator[TIn, Tout any](f func(TIn) events.Event[Tout], in Consumable[TIn], out streams.Stream[Tout]) OperatorControl {
	o := &Operator1[TIn, Tout]{
		id:     OperatorID(uuid.New()),
		f:      f,
		Input:  in,
		Output: out,
		active: false,
	}

	return o
}

func NewOperatorN[TIn, Tout any](f func(TIn) []events.Event[Tout], in Consumable[TIn], out streams.Stream[Tout]) OperatorControl {
	return &OperatorN[TIn, Tout]{
		id:     OperatorID(uuid.New()),
		f:      f,
		Input:  in,
		Output: out,
		active: false,
	}
}
