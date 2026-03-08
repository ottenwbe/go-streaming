package processing

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var (
	ErrPipelineOperatorInputOutput = errors.New("pipeline operator needs exactly 1 input and at least 1 output")
	ErrFilterOperatorInputOutput   = errors.New("filter operator needs exactly 1 input and at least 1 output")
	ErrMapOperatorInputOutput      = errors.New("map operator needs exactly 1 input and at least 1 output")
	ErrNilOperator                 = errors.New("operator is considered nil (either id or operator is nil)")
	ErrOperatorAlreadyExists       = errors.New("operator already exists")
	ErrFanInOperatorInputOutput    = errors.New("fan-in operator needs at least 2 inputs and at least 1 output")
)

func registerAndStartOperator(o OperatorEngine, autoStart bool) error {
	if err := OperatorRepository().put(o); err != nil {
		return err
	}

	if autoStart {
		if err := o.Start(); err != nil {
			OperatorRepository().remove(o)
			return err
		}
	}

	return nil
}

func NewPipelineOperator[TIn, TOut any](
	config OperatorConfig,
	operation func([]events.Event[TIn]) []TOut,
	id OperatorID,
) (OperatorID, error) {
	if len(config.Outputs) < 1 || len(config.Inputs) != 1 {
		return NilOperatorID(), ErrPipelineOperatorInputOutput
	}

	config.Type = PIPELINE_OPERATOR
	if id != NilOperatorID() {
		config.ID = id
	}

	o := &PipelineOperatorEngine[TIn, TOut]{
		baseOperatorEngine: baseOperatorEngine[TIn, TOut]{
			config: config,
		},
		operation: operation,
	}

	if err := registerAndStartOperator(o, config.AutoStart); err != nil {
		return NilOperatorID(), err
	}

	return o.ID(), nil
}

func NewFilterOperator[TIn any](
	config OperatorConfig,
	predicate func(events.Event[TIn]) bool,
	id OperatorID,
) (OperatorID, error) {
	if len(config.Outputs) < 1 || len(config.Inputs) != 1 {
		return NilOperatorID(), ErrFilterOperatorInputOutput
	}

	config.Type = FILTER_OPERATOR
	if id != NilOperatorID() {
		config.ID = id
	}

	o := &FilterOperatorEngine[TIn]{
		baseOperatorEngine: baseOperatorEngine[TIn, TIn]{
			config: config,
		},
		predicate: predicate,
	}

	if err := registerAndStartOperator(o, config.AutoStart); err != nil {
		return NilOperatorID(), err
	}

	return o.ID(), nil
}

func NewMapOperator[TIn, TOut any](
	config OperatorConfig,
	mapper func(events.Event[TIn]) TOut,
	id OperatorID,
) (OperatorID, error) {
	if len(config.Outputs) < 1 || len(config.Inputs) != 1 {
		return NilOperatorID(), ErrMapOperatorInputOutput
	}

	config.Type = MAP_OPERATOR
	if id != NilOperatorID() {
		config.ID = id
	}

	o := &MapOperatorEngine[TIn, TOut]{
		baseOperatorEngine: baseOperatorEngine[TIn, TOut]{
			config: config,
		},
		mapper: mapper,
	}

	if err := registerAndStartOperator(o, config.AutoStart); err != nil {
		return NilOperatorID(), err
	}

	return o.ID(), nil
}

func NewFanInOperator[TIn, TOut any](
	config OperatorConfig,
	fanInFunction func(map[int][]events.Event[TIn]) []TOut,
	id OperatorID,
) (OperatorID, error) {
	if len(config.Inputs) < 2 || len(config.Outputs) < 1 {
		return NilOperatorID(), ErrFanInOperatorInputOutput
	}

	config.Type = FANIN_OPERATOR
	if id != NilOperatorID() {
		config.ID = id
	}

	o := &FanInOperatorEngine[TIn, TOut]{
		baseOperatorEngine: baseOperatorEngine[TIn, TOut]{config: config},
		fanInFunction:      fanInFunction,
	}

	if err := registerAndStartOperator(o, config.AutoStart); err != nil {
		return NilOperatorID(), err
	}

	return o.ID(), nil
}

// NewJoinOperator is a blueprint for a factory function to create a heterogeneous JoinOperatorEngine.
func NewJoinOperator[TLeft, TRight, TOut any](
	config OperatorConfig,
	joinFunc func(left []events.Event[TLeft], right []events.Event[TRight]) []TOut,
) (OperatorID, error) {

	if len(config.Inputs) != 2 {
		return NilOperatorID(), errors.New("join operator requires exactly two input streams")
	}
	if len(config.Outputs) < 1 {
		return NilOperatorID(), errors.New("join operator requires at least one output stream")
	}

	engine := &JoinOperatorEngine[TLeft, TRight, TOut]{
		config:       config,
		joinFunction: joinFunc,
	}

	if err := registerAndStartOperator(engine, config.AutoStart); err != nil {
		return NilOperatorID(), err
	}

	return engine.ID(), nil
}

func RemoveOperator(oid OperatorID) error {
	o, found := OperatorRepository().Get(oid)
	if found {
		err := o.Stop()
		OperatorRepository().remove(o)
		return err
	}
	return nil
}

type ORepository interface {
	Get(id OperatorID) (OperatorEngine, bool)
	put(operator OperatorEngine) error
	List() map[OperatorID]OperatorEngine
	remove(operators OperatorEngine)
}

type defaultOperatorRepository struct {
	operators map[OperatorID]OperatorEngine
	mutex     sync.Mutex
}

func (m *defaultOperatorRepository) remove(operator OperatorEngine) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.operators, operator.ID())
}

func (m *defaultOperatorRepository) List() map[OperatorID]OperatorEngine {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.operators
}

func (m *defaultOperatorRepository) Get(id OperatorID) (OperatorEngine, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	o, ok := m.operators[id]
	return o, ok
}

func (m *defaultOperatorRepository) put(operator OperatorEngine) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if operator == nil || operator.ID() == OperatorID(uuid.Nil) {
		return ErrNilOperator
	}

	if _, ok := m.operators[operator.ID()]; ok {
		return ErrOperatorAlreadyExists
	}

	m.operators[operator.ID()] = operator

	return nil
}

var operatorRepository ORepository

func OperatorRepository() ORepository {
	return operatorRepository
}

func init() {
	operatorRepository = &defaultOperatorRepository{
		operators: make(map[OperatorID]OperatorEngine),
		mutex:     sync.Mutex{},
	}
}
