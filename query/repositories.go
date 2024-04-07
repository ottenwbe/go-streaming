package engine

import (
	"errors"
	"github.com/google/uuid"
	"go-stream-processing/query"
	"go-stream-processing/streams"
)

type (
	QueryControl struct {
		ID        query.QueryID
		Operators []OperatorControl
		Streams   []streams.StreamControl
	}
	QRepository interface {
		Get(id query.QueryID) (QueryControl, bool)
		put(q QueryControl) error
		remove(id query.QueryID)
		List() map[query.QueryID]QueryControl
	}
)

type concreteQueryRepository map[query.QueryID]QueryControl

func (c concreteQueryRepository) Get(id query.QueryID) (q QueryControl, ok bool) {
	q, ok = c[id]
	return
}

func (c concreteQueryRepository) remove(id query.QueryID) {
	delete(c, id)
}

func (c concreteQueryRepository) put(q QueryControl) error {
	if q.ID == query.QueryID(uuid.Nil) {
		return errors.New("invalid query id")
	}

	if _, ok := c.Get(q.ID); ok {
		return errors.New("query already exists")
	}

	c[q.ID] = q

	return nil
}

func (c concreteQueryRepository) List() map[query.QueryID]QueryControl {
	return c
}

type ORepository interface {
	Get(id OperatorID) (OperatorControl, bool)
	Put(operator OperatorControl) error
	List() map[OperatorID]OperatorControl
}

type MapRepository map[OperatorID]OperatorControl

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
