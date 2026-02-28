package processing

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/log"
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
	InStream(from pubsub.StreamID, description events.SelectionPolicyConfig)
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
	Input   pubsub.TypedSubscriber[TIN]
}

func (o *baseOperatorEngine[TIN, TOUT]) ID() OperatorID {
	return o.config.ID
}

func (o *baseOperatorEngine[TIN, TOUT]) InStream(in pubsub.StreamID, p events.SelectionPolicyConfig) {
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
				log.Errorf("PipelineOperator %s failed to publish: %v", o.ID(), err)
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
					log.Errorf("FilterOperator %s failed to publish: %v", o.ID(), err)
				}
			}
		}
	}
}

type FanInOperatorEngine[TIn any, TOut any] struct {
	baseOperatorEngine[TIn, TOut]
	fanInFunction func(map[int][]events.Event[TIn]) []TOut
	policy        events.MultiSelectionPolicy[TIn]
	buffers       map[int]*events.EventBuffer[TIn]
	inputs        []pubsub.TypedSubscriber[TIn]
	mutex         sync.Mutex
}

func (o *FanInOperatorEngine[TIn, TOut]) Start() error {
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

	o.buffers = make(map[int]*events.EventBuffer[TIn])
	readers := make(map[int]events.BufferReader[TIn])

	// Initialize policy based on the first input's configuration
	// We assume all inputs share the same windowing logic for the fan-in
	pDesc := o.config.Inputs[0].InputPolicy
	if pDesc.Type == events.TemporalWindow {
		o.policy = events.NewMultiTemporalWindowPolicy[TIn](pDesc.WindowStart, pDesc.WindowLength, pDesc.WindowShift)
	} else {
		return fmt.Errorf("unsupported policy for fan-in: %s", pDesc.Type)
	}

	for i, inputConfig := range o.config.Inputs {
		o.buffers[i] = events.NewEventBuffer[TIn]()
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

func (o *FanInOperatorEngine[TIn, TOut]) Stop() error {
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

func (o *FanInOperatorEngine[TIn, TOut]) processInput(idx int, e events.Event[TIn]) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if !o.active.Load() {
		return
	}

	o.buffers[idx].Add(e)
	o.policy.UpdateSelection()
}

func (o *FanInOperatorEngine[TIn, TOut]) onWindowReady(data map[int][]events.Event[TIn]) {
	var inputEvents []events.Event[TIn]
	for _, evs := range data {
		inputEvents = append(inputEvents, evs...)
	}

	results := o.fanInFunction(data)
	for _, res := range results {
		ce := events.NewEventFromOthers(res, events.GetTimeStamps(inputEvents...)...)
		for _, pub := range o.Outputs {
			if err := pub.Publish(ce); err != nil {
				log.Errorf("FanInOperator %s failed to publish: %v", o.ID(), err)
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

// JoinOperatorEngine is a blueprint for a specialized engine to join two streams of potentially different types.
type JoinOperatorEngine[TLeft, TRight, TOut any] struct {
	config       OperatorConfig
	active       atomic.Bool
	outputs      []pubsub.Publisher[TOut]
	subLeft      pubsub.TypedSubscriber[TLeft]
	subRight     pubsub.TypedSubscriber[TRight]
	joinFunction func(left []events.Event[TLeft], right []events.Event[TRight]) []TOut
	policy       events.DuoPolicy[TLeft, TRight]
	bufferLeft   *events.EventBuffer[TLeft]
	bufferRight  *events.EventBuffer[TRight]
	mutex        sync.Mutex
}

func (o *JoinOperatorEngine[TLeft, TRight, TOut]) ID() OperatorID {
	return o.config.ID
}

func (o *JoinOperatorEngine[TLeft, TRight, TOut]) InStream(from pubsub.StreamID, description events.SelectionPolicyConfig) {
	o.config.Inputs = append(o.config.Inputs, InputConfig{
		from,
		description,
	})
}
func (o *JoinOperatorEngine[TLeft, TRight, TOut]) OutStream(to pubsub.StreamID) {
	o.config.Outputs = append(o.config.Outputs, to)
}

func (o *JoinOperatorEngine[TLeft, TRight, TOut]) Start() error {
	if !o.active.CompareAndSwap(false, true) {
		return nil
	}

	o.outputs = make([]pubsub.Publisher[TOut], 0, len(o.config.Outputs))
	for _, outID := range o.config.Outputs {
		pub, err := pubsub.RegisterPublisher[TOut](outID)
		if err != nil {
			for _, p := range o.outputs {
				_ = pubsub.UnRegisterPublisher(p)
			}
			o.active.Store(false)
			return err
		}
		o.outputs = append(o.outputs, pub)
	}

	o.bufferLeft = events.NewEventBuffer[TLeft]()
	o.bufferRight = events.NewEventBuffer[TRight]()

	pDesc := o.config.Inputs[0].InputPolicy
	if pDesc.Type == events.TemporalWindow {
		o.policy = events.NewDuoTemporalWindowPolicy[TLeft, TRight](pDesc.WindowStart, pDesc.WindowLength, pDesc.WindowShift)
	} else {
		for _, p := range o.outputs {
			_ = pubsub.UnRegisterPublisher(p)
		}
		o.active.Store(false)
		return fmt.Errorf("unsupported policy for join: %s", pDesc.Type)
	}

	o.policy.SetBuffers(o.bufferLeft, o.bufferRight)
	o.policy.AddCallback(o.onWindowReady)

	var err error
	o.subLeft, err = pubsub.SubscribeByTopicID[TLeft](o.config.Inputs[0].Stream, func(e events.Event[TLeft]) {
		o.processLeft(e)
	})
	if err != nil {
		o.Stop()
		return err
	}

	o.subRight, err = pubsub.SubscribeByTopicID[TRight](o.config.Inputs[1].Stream, func(e events.Event[TRight]) {
		o.processRight(e)
	})
	if err != nil {
		o.Stop()
		return err
	}

	return nil
}

func (o *JoinOperatorEngine[TLeft, TRight, TOut]) Stop() error {
	o.active.Store(false)
	var errs []error
	if o.subLeft != nil {
		errs = append(errs, pubsub.Unsubscribe(o.subLeft))
	}
	if o.subRight != nil {
		errs = append(errs, pubsub.Unsubscribe(o.subRight))
	}
	for _, pub := range o.outputs {
		if err := pubsub.UnRegisterPublisher(pub); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (o *JoinOperatorEngine[TLeft, TRight, TOut]) processLeft(e events.Event[TLeft]) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if !o.active.Load() {
		return
	}
	o.bufferLeft.Add(e)
	o.policy.UpdateSelection()
}

func (o *JoinOperatorEngine[TLeft, TRight, TOut]) processRight(e events.Event[TRight]) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if !o.active.Load() {
		return
	}
	o.bufferRight.Add(e)
	o.policy.UpdateSelection()
}

func (o *JoinOperatorEngine[TLeft, TRight, TOut]) onWindowReady(left []events.Event[TLeft], right []events.Event[TRight]) {
	results := o.joinFunction(left, right)

	var allStamps []events.TimeStamp
	for _, e := range left {
		allStamps = append(allStamps, e.GetStamp())
	}
	for _, e := range right {
		allStamps = append(allStamps, e.GetStamp())
	}

	for _, res := range results {
		ce := events.NewEventFromOthers(res, allStamps...)
		for _, pub := range o.outputs {
			_ = pub.Publish(ce)
		}
	}

	selLeft, selRight := o.policy.NextSelection()

	removeLeft := selLeft.Start
	if removeLeft > 0 {
		o.bufferLeft.Remove(removeLeft)
	}

	removeRight := selRight.Start
	if removeRight > 0 {
		o.bufferRight.Remove(removeRight)
	}

	o.policy.Offset(removeLeft, removeRight)
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
				log.Errorf("MapOperator %s failed to publish: %v", o.ID(), err)
			}
		}
	}
}

func (o *MapOperatorEngine[TIN, TOUT]) Process(in ...events.Event[TIN]) {
	for _, event := range in {
		o.ProcessSingleEvent(event)
	}
}
