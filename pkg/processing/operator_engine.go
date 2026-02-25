package processing

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

type OperatorID uuid.UUID

func (o OperatorID) String() string {
	return o.String()
}

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
	InStream(from pubsub.StreamID, description events.PolicyDescription)
	OutStream(to pubsub.StreamID)
}

type TypedOperatorExecutor[TIn any] interface {
	OperatorEngine
	Process(event ...events.Event[TIn])
}

type baseOperatorEngine[TIN any, TOUT any] struct {
	config  OperatorConfig
	active  atomic.Bool
	Outputs []pubsub.Publisher[TOUT]
	Input   pubsub.Subscriber[TIN]
}

func (o *baseOperatorEngine[TIN, TOUT]) ID() OperatorID {
	return o.config.ID
}

func (o *baseOperatorEngine[TIN, TOUT]) InStream(in pubsub.StreamID, p events.PolicyDescription) {
	o.config.Inputs = append(o.config.Inputs, InputConfig{
		in,
		p,
	})
}

func (o *baseOperatorEngine[TIN, TOUT]) OutStream(to pubsub.StreamID) {
	o.config.Outputs = append(o.config.Outputs, to)
}

func (o *baseOperatorEngine[TIN, TOUT]) start(processFunc func(in ...events.Event[TIN])) error {
	if !o.active.CompareAndSwap(false, true) {
		return nil
	}

	inID := o.config.Inputs[0].Stream
	policy := o.config.Inputs[0].InputPolicy

	o.Outputs = make([]pubsub.Publisher[TOUT], 0, len(o.config.Outputs))
	for _, outID := range o.config.Outputs {
		pub, err := pubsub.RegisterPublisher[TOUT](outID)
		if err != nil {
			for _, p := range o.Outputs {
				_ = pubsub.UnRegisterPublisher(p)
			}
			o.active.Store(false)
			return err
		}
		o.Outputs = append(o.Outputs, pub)
	}

	var err error
	o.Input, err = pubsub.SubscribeBatchByTopicID[TIN](
		inID,
		processFunc,
		pubsub.SubscriberIsSync(false),
		pubsub.SubscriberWithSelectionPolicy(policy))

	if err != nil {
		for _, p := range o.Outputs {
			_ = pubsub.UnRegisterPublisher(p)
		}
		o.active.Store(false)
		return err
	}

	return nil
}

func (o *baseOperatorEngine[TIN, TOUT]) Stop() error {
	o.active.Store(false)
	err := pubsub.Unsubscribe(o.Input)
	var errs []error
	if err != nil {
		errs = append(errs, err)
	}
	for _, pub := range o.Outputs {
		if e := pubsub.UnRegisterPublisher(pub); e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
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
		for _, pub := range o.Outputs {
			if err := pub.Publish(ce); err != nil {
				// events.Errorf("PipelineOperator %s failed to publish: %v", o.ID(), err)
			}
		}
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
			for _, pub := range o.Outputs {
				if err := pub.Publish(event); err != nil {
					// events.Errorf("FilterOperator %s failed to publish: %v", o.ID(), err)
				}
			}
		}
	}
}

type JoinOperatorEngine[TIn any, TOut any] struct {
	baseOperatorEngine[TIn, TOut]
	joinFunction func(map[int][]events.Event[TIn]) []TOut
	policy       events.MultiPolicy[TIn]
	buffers      map[int]*joinBuffer[TIn]
	inputs       []pubsub.Subscriber[TIn]
	mutex        sync.Mutex
}

type joinBuffer[T any] struct {
	events []events.Event[T]
}

func (j *joinBuffer[T]) Get(i int) events.Event[T] {
	return j.events[i]
}

func (j *joinBuffer[T]) Len() int {
	return len(j.events)
}

func (j *joinBuffer[T]) Add(e events.Event[T]) {
	j.events = append(j.events, e)
}

func (j *joinBuffer[T]) Remove(n int) {
	for i := 0; i < n; i++ {
		j.events[i] = nil
	}
	j.events = j.events[n:]
}

func (o *JoinOperatorEngine[TIn, TOut]) Start() error {
	if !o.active.CompareAndSwap(false, true) {
		return nil
	}

	o.Outputs = make([]pubsub.Publisher[TOut], 0, len(o.config.Outputs))
	for _, outID := range o.config.Outputs {
		pub, err := pubsub.RegisterPublisher[TOut](outID)
		if err != nil {
			// if we fail, unregister any publishers we already created
			for _, p := range o.Outputs {
				_ = pubsub.UnRegisterPublisher(p)
			}
			o.active.Store(false)
			return err
		}
		o.Outputs = append(o.Outputs, pub)
	}

	o.buffers = make(map[int]*joinBuffer[TIn])
	readers := make(map[int]events.BufferReader[TIn])

	// Initialize policy based on the first input's configuration
	// We assume all inputs share the same windowing logic for the join
	pDesc := o.config.Inputs[0].InputPolicy
	if pDesc.Type == events.TemporalWindow {
		o.policy = events.NewMultiTemporalWindowPolicy[TIn](pDesc.WindowStart, pDesc.WindowLength, pDesc.WindowShift)
	} else {
		return fmt.Errorf("unsupported policy for join: %s", pDesc.Type)
	}

	for i, inputConfig := range o.config.Inputs {
		o.buffers[i] = &joinBuffer[TIn]{}
		readers[i] = o.buffers[i]

		idx := i
		sub, err := pubsub.SubscribeByTopicID[TIn](inputConfig.Stream, func(e events.Event[TIn]) {
			o.processInput(idx, e)
		})
		if err != nil {
			// if we fail, clean up everything we've created so far
			for _, p := range o.Outputs {
				_ = pubsub.UnRegisterPublisher(p)
			}
			for _, s := range o.inputs {
				_ = pubsub.Unsubscribe(s)
			}
			o.active.Store(false)
			return err
		}
		o.inputs = append(o.inputs, sub)
	}

	o.policy.SetBuffers(readers)
	o.policy.AddCallback(o.onWindowReady)

	return nil
}

func (o *JoinOperatorEngine[TIn, TOut]) Stop() error {
	o.active.Store(false)
	var errs []error
	for _, sub := range o.inputs {
		if err := pubsub.Unsubscribe(sub); err != nil {
			errs = append(errs, err)
		}
	}
	for _, pub := range o.Outputs {
		if err := pubsub.UnRegisterPublisher(pub); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (o *JoinOperatorEngine[TIn, TOut]) processInput(idx int, e events.Event[TIn]) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if !o.active.Load() {
		return
	}

	o.buffers[idx].Add(e)
	o.policy.UpdateSelection()
}

func (o *JoinOperatorEngine[TIn, TOut]) onWindowReady(data map[int][]events.Event[TIn]) {
	var inputEvents []events.Event[TIn]
	for _, evs := range data {
		inputEvents = append(inputEvents, evs...)
	}

	results := o.joinFunction(data)
	for _, res := range results {
		ce := events.NewEventFromOthers(res, events.GetTimeStamps(inputEvents...)...)
		for _, pub := range o.Outputs {
			if err := pub.Publish(ce); err != nil {
				// events.Errorf("JoinOperator %s failed to publish: %v", o.ID(), err)
			}
		}
	}

	// Garbage collection: remove events that were processed and are no longer needed
	selection := o.policy.NextSelection()
	for id, sel := range selection {
		removeCount := sel.Start
		if removeCount > 0 {
			o.buffers[id].Remove(removeCount)
			o.policy.Offset(id, removeCount)
		}
	}

	o.policy.Shift()
}

type MapOperatorEngine[TIN any, TOUT any] struct {
	baseOperatorEngine[TIN, TOUT]
	mapper func(event events.Event[TIN]) TOUT
}

func (o *MapOperatorEngine[TIN, TOUT]) Start() error {
	if !o.active.CompareAndSwap(false, true) {
		return nil
	}

	inID := o.config.Inputs[0].Stream

	o.Outputs = make([]pubsub.Publisher[TOUT], 0, len(o.config.Outputs))
	for _, outID := range o.config.Outputs {
		pub, err := pubsub.RegisterPublisher[TOUT](outID)
		if err != nil {
			for _, p := range o.Outputs {
				_ = pubsub.UnRegisterPublisher(p)
			}
			o.active.Store(false)
			return err
		}
		o.Outputs = append(o.Outputs, pub)
	}

	var err error
	o.Input, err = pubsub.SubscribeByTopicID[TIN](
		inID,
		o.ProcessSingleEvent,
		pubsub.SubscriberIsSync(false))
	if err != nil {
		// if subscription fails, we need to clean up the publishers we just created
		for _, p := range o.Outputs {
			_ = pubsub.UnRegisterPublisher(p)
		}
		o.active.Store(false)
		return err
	}
	return nil
}

func (o *MapOperatorEngine[TIN, TOUT]) ProcessSingleEvent(event events.Event[TIN]) {
	if event != nil {
		out := events.NewEventFromOthers(o.mapper(event), event.GetStamp())
		for _, pub := range o.Outputs {
			if err := pub.Publish(out); err != nil {
				// events.Errorf("MapOperator %s failed to publish: %v", o.ID(), err)
			}
		}
	}
}

func (o *MapOperatorEngine[TIN, TOUT]) Process(in ...events.Event[TIN]) {
	for _, event := range in {
		o.ProcessSingleEvent(event)
	}
}
