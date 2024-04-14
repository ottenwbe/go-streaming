package engine

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/internal/buffer"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
	"go-stream-processing/pkg/selection"
	"go.uber.org/zap"
)

type (
	OperatorID uuid.UUID

	Consumable[T any] interface {
		Consume() T
		Run()
		Close()
	}
	OperatorStreamSubscription[TSub any] struct {
		streamReceiver *pubsub.StreamReceiver[TSub]
		streamID       pubsub.StreamID
		inputBuffer    buffer.Buffer[TSub]
		active         bool
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

func NewSingleStreamInputN[TStream any](inputStream pubsub.StreamID, policy selection.Policy[TStream]) Consumable[SingleStreamSelectionN[TStream]] {
	inputSub := &OperatorStreamSubscription[TStream]{
		streamID:    inputStream,
		inputBuffer: buffer.NewConsumableAsyncBuffer[TStream](policy),
	}

	return &SingleStreamInputN[SingleStreamSelectionN[TStream], TStream]{
		Subscription: inputSub,
	}
}

func NewDoubleStreamInput1[TIN1, TIN2 any](inputStream1 pubsub.StreamID, inputStream2 pubsub.StreamID) Consumable[DoubleInputSelection1[TIN1, TIN2]] {
	inputSub1 := &OperatorStreamSubscription[TIN1]{
		streamID:    inputStream1,
		inputBuffer: buffer.NewSimpleAsyncBuffer[TIN1](),
	}

	inputSub2 := &OperatorStreamSubscription[TIN2]{
		streamID:    inputStream2,
		inputBuffer: buffer.NewSimpleAsyncBuffer[TIN2](),
	}

	inStream := &DoubleStreamInput1[DoubleInputSelection1[TIN1, TIN2], TIN1, TIN2]{
		Subscription1: inputSub1,
		Subscription2: inputSub2,
	}
	return inStream
}

func NewDoubleStreamInputN[TIN1, TIN2 any](inputStream1 pubsub.StreamID, selection1 selection.Policy[TIN1], inputStream2 pubsub.StreamID, selection2 selection.Policy[TIN2]) *DoubleStreamInputN[DoubleInputSelectionN[TIN1, TIN2], TIN1, TIN2] {
	inputSub1 := &OperatorStreamSubscription[TIN1]{
		streamID:    inputStream1,
		inputBuffer: buffer.NewConsumableAsyncBuffer[TIN1](selection1),
	}

	inputSub2 := &OperatorStreamSubscription[TIN2]{
		streamID:    inputStream2,
		inputBuffer: buffer.NewConsumableAsyncBuffer[TIN2](selection2),
	}

	inStream := &DoubleStreamInputN[DoubleInputSelectionN[TIN1, TIN2], TIN1, TIN2]{
		Subscription1: inputSub1,
		Subscription2: inputSub2,
	}
	return inStream
}

func NewSingleStreamInput1[TStream any](inputStream pubsub.StreamID) Consumable[SingleStreamSelection1[TStream]] {
	inputSub := &OperatorStreamSubscription[TStream]{
		streamID:    inputStream,
		inputBuffer: buffer.NewSimpleAsyncBuffer[TStream](),
	}

	return &SingleStreamInput1[SingleStreamSelection1[TStream], TStream]{
		Subscription: inputSub,
	}
}

func (s *SingleStreamInputN[TRes, TStream]) Consume() TRes {
	return s.Subscription.inputBuffer.GetAndConsumeNextEvents()
}

func (s *SingleStreamInputN[TRes, TStream]) Run() {
	s.Subscription.Run()
}

func (s *SingleStreamInputN[TRes, TStream]) Close() {
	s.Subscription.Close()
}

func (s *SingleStreamInput1[TRes, TStream]) Consume() TRes {
	return s.Subscription.inputBuffer.GetAndRemoveNextEvent().(TRes)
}

func (s *SingleStreamInput1[TRes, TStream]) Run() {
	s.Subscription.Run()
}

func (s *SingleStreamInput1[TRes, TStream]) Close() {
	s.Subscription.Close()
}

func (s *DoubleStreamInputN[TRes, TStream1, TStream2]) Consume() TRes {
	return TRes{
		input1: s.Subscription1.inputBuffer.GetAndConsumeNextEvents(),
		input2: s.Subscription2.inputBuffer.GetAndConsumeNextEvents(),
	}
}

func (s *DoubleStreamInputN[TRes, TStream1, TStream2]) Run() {
	s.Subscription1.Run()
	s.Subscription2.Run()
}

func (s *DoubleStreamInputN[TRes, TStream1, TStream2]) Close() {
	s.Subscription1.Close()
	s.Subscription2.Close()
}

func (d *DoubleStreamInput1[TRes, TStream1, TStream2]) Consume() TRes {
	return TRes{
		Input1: d.Subscription1.inputBuffer.GetAndRemoveNextEvent(),
		Input2: d.Subscription2.inputBuffer.GetAndRemoveNextEvent(),
	}
}

func (d *DoubleStreamInput1[TRes, TStream1, TStream2]) Run() {
	d.Subscription1.Run()
	d.Subscription2.Run()
}

func (d *DoubleStreamInput1[TRes, TStream1, TStream2]) Close() {
	d.Subscription1.Close()
	d.Subscription2.Close()
}

func (o *OperatorStreamSubscription[T]) Close() {
	if o.active == true {
		pubsub.Unsubscribe[T](o.streamReceiver)
		o.inputBuffer.StopBlocking()
	}
}

func (o *OperatorStreamSubscription[T]) Run() {

	if o.active {
		return
	}
	o.active = true

	var err error
	o.streamReceiver, err = pubsub.Subscribe[T](o.streamID)
	if err != nil {
		zap.S().Error("operator subscription failed", zap.Error(err), zap.String("stream", o.streamID.String()))
		return
	}

	go func() {
		for {
			event, more := <-o.streamReceiver.Notify
			if more {
				o.inputBuffer.AddEvent(event)
			} else {
				return
			}
		}
	}()
}

func (o *OperatorStreamSubscription[T]) Consume() []events.Event[T] {
	return o.inputBuffer.GetAndConsumeNextEvents()
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
	fin    chan bool

	Input  Consumable[TIN]
	Output pubsub.Publisher[TOUT]
}

type OperatorN[TIN any, TOUT any] struct {
	id OperatorID
	f  func(in TIN) []events.Event[TOUT]

	active bool
	fin    chan bool

	Input  Consumable[TIN]
	Output pubsub.Publisher[TOUT]
}

func (op *Operator1[TIn, Tout]) Stop() {
	if op.active == true {
		op.active = false
		op.Input.Close()
	}
}

func (op *Operator1[TIN, TOUT]) ID() OperatorID {
	return op.id
}

func (op *Operator1[TIn, Tout]) Start() {

	if !op.active {
		op.active = true

		op.Input.Run()

		go func() {

			defer func() { op.fin <- true }()

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

func (op *OperatorN[TIN, TOUT]) Stop() {
	if op.active == true {
		op.active = false
		op.Input.Close()
	}
}

func (op *OperatorN[TIN, TOUT]) ID() OperatorID {
	return op.id
}

func (op *OperatorN[TIN, TOUT]) Start() {
	if !op.active {
		op.active = true

		op.Input.Run()

		go func() {

			defer func() { op.fin <- true }()

			var (
				inputEvents TIN
				publishErr  error
			)

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

func NewOperator[TIn, Tout any](f func(TIn) events.Event[Tout], in Consumable[TIn], out pubsub.Publisher[Tout]) OperatorControl {
	o := &Operator1[TIn, Tout]{
		id:     OperatorID(uuid.New()),
		f:      f,
		Input:  in,
		Output: out,
		active: false,
		fin:    make(chan bool),
	}

	return o
}

func NewOperatorN[TIn, Tout any](f func(TIn) []events.Event[Tout], in Consumable[TIn], out pubsub.Publisher[Tout]) OperatorControl {
	return &OperatorN[TIn, Tout]{
		id:     OperatorID(uuid.New()),
		f:      f,
		Input:  in,
		Output: out,
		active: false,
		fin:    make(chan bool),
	}
}

type ORepository interface {
	Get(id OperatorID) (OperatorControl, bool)
	Put(operator OperatorControl) error
	List() map[OperatorID]OperatorControl
	Remove(operators []OperatorControl)
}

type MapRepository map[OperatorID]OperatorControl

func (m MapRepository) Remove(operators []OperatorControl) {
	for _, operators := range operators {
		delete(m, operators.ID())
	}
}

func (m MapRepository) List() map[OperatorID]OperatorControl {
	return m
}

func (m MapRepository) Get(id OperatorID) (OperatorControl, bool) {
	o, ok := m[id]
	return o, ok
}

func (m MapRepository) Put(operator OperatorControl) error {

	if operator == nil || operator.ID() == OperatorID(uuid.Nil) {
		return errors.New("operator is considered nil (either id or operator is nil)")
	}

	if _, ok := m.Get(operator.ID()); ok {
		return errors.New("operator already exists")
	}

	m[operator.ID()] = operator

	return nil
}

var operatorRepository ORepository

func OperatorRepository() ORepository {
	return operatorRepository
}

func init() {
	operatorRepository = MapRepository{}
}
