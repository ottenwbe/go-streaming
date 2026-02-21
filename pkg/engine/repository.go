package engine

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var (
	ErrPipelineOperatorInputOutput = errors.New("pipeline operator needs exactly length 1 for inputs and outputs")
	ErrInvalidPipelineOperation    = errors.New("invalid operation type for pipeline operator, expected func([]events.Event[TIn]) []Tout")
	ErrFilterOperatorInputOutput   = errors.New("filter operator needs exactly 1 input and 1 output")
	ErrInvalidFilterPredicate      = errors.New("invalid predicate type for filter operator, expected func(events.Event[TIn]) bool")
	ErrUnknownOperatorType         = errors.New("unknown operator type")
	ErrMapOperatorInputOutput      = errors.New("map operator needs exactly 1 input and 1 output")
	ErrInvalidMapOperation         = errors.New("invalid operation type for map operator, expected func(events.Event[TIn]) Tout")
	ErrNilOperator                 = errors.New("operator is considered nil (either id or operator is nil)")
	ErrFanoutOperatorInputOutput   = errors.New("fanout operator needs exactly 1 input and at least 1 output")
	ErrOperatorAlreadyExists       = errors.New("operator already exists")
)

func NewOperator[TIn, Tout any](operation any, d *OperatorDescription) (OperatorID, error) {
	var o OperatorEngine

	switch d.Type {
	case PIPELINE_OPERATOR:
		// validate
		if len(d.Outputs) != 1 || len(d.Inputs) != 1 {
			return NilOperatorID(), ErrPipelineOperatorInputOutput
		}

		op, ok := operation.(func([]events.Event[TIn]) []Tout)
		if !ok {
			return NilOperatorID(), ErrInvalidPipelineOperation
		}

		// create
		o = &PipelineOperatorEngine[TIn, Tout]{
			baseOperatorEngine: baseOperatorEngine[TIn, Tout]{
				config: d,
			},
			operation: op,
		}
	case FILTER_OPERATOR:
		// validate
		if len(d.Outputs) != 1 || len(d.Inputs) != 1 {
			return NilOperatorID(), ErrFilterOperatorInputOutput
		}

		predicate, ok := operation.(func(events.Event[TIn]) bool)
		if !ok {
			return NilOperatorID(), ErrInvalidFilterPredicate
		}

		// create
		o = &FilterOperatorEngine[TIn]{
			baseOperatorEngine: baseOperatorEngine[TIn, TIn]{
				config: d,
			},
			predicate: predicate,
		}
	case MAP_OPERATOR:
		// validate
		if len(d.Outputs) != 1 || len(d.Inputs) != 1 {
			return NilOperatorID(), ErrMapOperatorInputOutput
		}

		mapper, ok := operation.(func(events.Event[TIn]) Tout)
		if !ok {
			return NilOperatorID(), ErrInvalidMapOperation
		}

		// create
		o = &MapOperatorEngine[TIn, Tout]{
			baseOperatorEngine: baseOperatorEngine[TIn, Tout]{
				config: d,
			},
			mapper: mapper,
		}
	case FANOUT_OPERATOR:
		// validate
		if len(d.Inputs) != 1 || len(d.Outputs) < 1 {
			return NilOperatorID(), ErrFanoutOperatorInputOutput
		}
		o = &FanOutOperatorEngine[TIn]{
			config: d,
		}
	default:
		return NilOperatorID(), fmt.Errorf("%w: %s", ErrUnknownOperatorType, d.Type)
	}

	err := OperatorRepository().put(o)
	if err != nil {
		return NilOperatorID(), err
	}

	if d.AutoStart {
		err = o.Start()
	}
	return o.ID(), err
}

func RemoveOperator(oid OperatorID) {
	o, found := OperatorRepository().Get(oid)
	if found {
		_ = o.Stop()
		OperatorRepository().remove(o)
	}
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
