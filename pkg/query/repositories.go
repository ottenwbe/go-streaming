package query

import (
	"errors"
	"github.com/google/uuid"
)

type ID uuid.UUID

func (q ID) String() string {
	return q.String()
}

type (
	QRepository interface {
		Get(id ID) (*QueryControl, bool)
		put(q *QueryControl) error
		remove(id ID)
		List() map[ID]*QueryControl
	}
)

type concreteQueryRepository map[ID]*QueryControl

func (c concreteQueryRepository) Get(id ID) (q *QueryControl, ok bool) {
	q, ok = c[id]
	return
}

func (c concreteQueryRepository) remove(id ID) {
	delete(c, id)
}

func (c concreteQueryRepository) put(q *QueryControl) error {
	if q.ID == ID(uuid.Nil) {
		return errors.New("invalid query id")
	}

	if _, ok := c.Get(q.ID); ok {
		return errors.New("query already exists")
	}

	c[q.ID] = q

	return nil
}

func (c concreteQueryRepository) List() map[ID]*QueryControl {
	return c
}

var (
	queryRepository QRepository
)

func QueryRepository() QRepository {
	return queryRepository
}

func init() {
	queryRepository = concreteQueryRepository{}
}
