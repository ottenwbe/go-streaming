package engine

import (
	"errors"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

type OperatorID uuid.UUID

func NewOperatorID() OperatorID {
	return OperatorID(uuid.New())
}

func NilOperatorID() OperatorID {
	return OperatorID(uuid.Nil)
}

type OperatorEngine interface {
	ID() OperatorID
	Start() error
	Stop() error
}

type TypedOperatorExecutor[TIn any] interface {
	OperatorEngine
	Process(event ...events.Event[TIn])
}

type baseOperatorEngine[TIN any, TOUT any] struct {
	config *OperatorDescription
	active atomic.Bool
	Output pubsub.Publisher[TOUT]
	Input  pubsub.Subscriber[TIN]
}

func (o *baseOperatorEngine[TIN, TOUT]) ID() OperatorID {
	return o.config.ID
}

func (o *baseOperatorEngine[TIN, TOUT]) start(processFunc func(in ...events.Event[TIN])) error {
	o.active.Store(true)

	outID := o.config.Outputs[0]
	inID := o.config.Inputs[0].Stream
	policy := o.config.Inputs[0].InputPolicy

	var err error
	o.Output, err = pubsub.RegisterPublisherByTopic[TOUT](outID)
	if err != nil {
		return err
	}
	o.Input, err = pubsub.SubscribeBatchByTopic[TIN](
		inID,
		processFunc,
		pubsub.SubscriberIsSync(false),
		pubsub.SubscriberWithSelectionPolicy(policy))
	return err
}

func (o *baseOperatorEngine[TIN, TOUT]) Stop() error {
	o.active.Store(false)
	err := pubsub.Unsubscribe(o.Input)
	err2 := pubsub.UnRegisterPublisher(o.Output)
	return errors.Join(err, err2)
}

type PipelineOperatorEngine[TIN any, TOUT any] struct {
	baseOperatorEngine[TIN, TOUT]
	operation func(event []events.Event[TIN]) []TOUT
}

func (o *PipelineOperatorEngine[TIN, TOUT]) Start() error {
	return o.baseOperatorEngine.start(o.Process)
}

func (o *PipelineOperatorEngine[TIN, TOUT]) Process(in ...events.Event[TIN]) {
	result := o.operation(in)
	for _, r := range result {
		ce := events.NewEventFromOthers(r, events.GetTimeStamps(in...)...)
		_ = o.Output.PublishComplex(ce)
	}
}

type FilterOperatorEngine[TIN any] struct {
	baseOperatorEngine[TIN, TIN]
	predicate func(event events.Event[TIN]) bool
}

func (o *FilterOperatorEngine[TIN]) Start() error {
	return o.baseOperatorEngine.start(o.Process)
}

func (o *FilterOperatorEngine[TIN]) Process(in ...events.Event[TIN]) {
	for _, event := range in {
		if event != nil && o.predicate(event) {
			_ = o.baseOperatorEngine.Output.PublishComplex(event)
		}
	}
}

type MapOperatorEngine[TIN any, TOUT any] struct {
	baseOperatorEngine[TIN, TOUT]
	mapper func(event events.Event[TIN]) TOUT
}

func (o *MapOperatorEngine[TIN, TOUT]) Start() error {
	o.active.Store(true)

	outID := o.config.Outputs[0]
	inID := o.config.Inputs[0].Stream

	var err error
	o.Output, err = pubsub.RegisterPublisherByTopic[TOUT](outID)
	if err != nil {
		return err
	}

	o.Input, err = pubsub.SubscribeByTopic[TIN](
		inID,
		o.ProcessSingleEvent,
		pubsub.SubscriberIsSync(false))
	return err
}

func (o *MapOperatorEngine[TIN, TOUT]) ProcessSingleEvent(event events.Event[TIN]) {
	if event != nil {
		out := events.NewEventFromOthers(o.mapper(event), event.GetStamp())
		_ = o.baseOperatorEngine.Output.PublishComplex(out)
	}
}

func (o *MapOperatorEngine[TIN, TOUT]) Process(in ...events.Event[TIN]) {
	for _, event := range in {
		o.ProcessSingleEvent(event)
	}
}
