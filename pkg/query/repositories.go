package query

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/internal/engine"
)

type (
	QRepository interface {
		Get(id QueryID) (*QueryControl, bool)
		put(q *QueryControl) error
		remove(id QueryID)
		List() map[QueryID]*QueryControl
	}
)

type concreteQueryRepository map[QueryID]*QueryControl

func (c concreteQueryRepository) Get(id QueryID) (q *QueryControl, ok bool) {
	q, ok = c[id]
	return
}

func (c concreteQueryRepository) remove(id QueryID) {
	delete(c, id)
}

func (c concreteQueryRepository) put(q *QueryControl) error {
	if q.ID == QueryID(uuid.Nil) {
		return errors.New("invalid query id")
	}

	if _, ok := c.Get(q.ID); ok {
		return errors.New("query already exists")
	}

	c[q.ID] = q

	return nil
}

func (c concreteQueryRepository) List() map[QueryID]*QueryControl {
	return c
}

type ORepository interface {
	Get(id engine.OperatorID) (engine.OperatorControl, bool)
	Put(operator engine.OperatorControl) error
	List() map[engine.OperatorID]engine.OperatorControl
}

type MapRepository map[engine.OperatorID]engine.OperatorControl

func (m MapRepository) List() map[engine.OperatorID]engine.OperatorControl {
	return m
}

func (m MapRepository) Get(id engine.OperatorID) (engine.OperatorControl, bool) {
	o, ok := m[id]
	return o, ok
}

func (m MapRepository) Put(operator engine.OperatorControl) error {

	if operator == nil || operator.ID() == engine.OperatorID(uuid.Nil) {
		return errors.New("operator is considered nil (either id or operator is nil)")
	}

	if _, ok := m.Get(operator.ID()); ok {
		return errors.New("operator already exists")
	}

	m[operator.ID()] = operator

	return nil
}

var (
	operatorRepository ORepository
	queryRepository    QRepository
)

func OperatorRepository() ORepository {
	return operatorRepository
}

func QueryRepository() QRepository {
	return queryRepository
}

func init() {
	operatorRepository = MapRepository{}
	queryRepository = concreteQueryRepository{}
}
