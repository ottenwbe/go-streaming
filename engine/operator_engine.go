package engine

import (
	"github.com/google/uuid"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
	"go-stream-processing/streams"
)

type OperatorStreamSubscription struct {
	Stream      streams.StreamReceiver
	InputBuffer buffer.Buffer
	Selection   buffer.SelectionPolicy
}

func (o *OperatorStreamSubscription) Run() {
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

func (o *OperatorStreamSubscription) Consume() []events.Event {
	return o.Selection.Apply(o.InputBuffer)
}

type Operator struct {
	id uuid.UUID
	f  func(events map[string][]events.Event) []events.Event

	active bool

	Input  map[string]*OperatorStreamSubscription
	Output []uuid.UUID
}

func NewOperator(f func(events map[string][]events.Event) []events.Event, Input map[string]*OperatorStreamSubscription, Output []uuid.UUID) *Operator {
	return &Operator{
		id:     uuid.New(),
		f:      f,
		Input:  Input,
		Output: Output,
		active: false,
	}
}

func (op *Operator) Stop() {
	op.active = false
}

func (op *Operator) Start() {

	if !op.active {
		op.active = true

		go func() {
			inputEventMap := make(map[string][]events.Event)
			for op.active {
				for stream, _ := range op.Input {
					inputEventMap[stream] = op.Input[stream].Consume()
				}
				result := op.f(inputEventMap)
				for _, streamID := range op.Output {
					for _, event := range result {
						if err := streams.PubSubSystem.Publish(streamID, event); err != nil {
							//TODO: log
						}
					}
				}
			}
		}()
	}
}
