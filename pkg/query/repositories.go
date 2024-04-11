package query

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

type ID uuid.UUID

func (q ID) String() string {
	return uuid.UUID(q).String()
}

type (
	Repository interface {
		fmt.Stringer
		Get(id ID) (*QueryControl, bool)
		put(q *QueryControl) error
		remove(id ID)
		List() map[ID]*QueryControl
	}
)

type concreteQueryRepository map[ID]*QueryControl

func (c concreteQueryRepository) String() string {

	s := "QueryControl {"
	for id, _ := range c.List() {
		s = s + fmt.Sprintf(" %v ", id)
	}
	s += "}"
	return s
}

func (c concreteQueryRepository) Get(id ID) (q *QueryControl, ok bool) {
	q, ok = c[id]
	return
}

func (c concreteQueryRepository) remove(id ID) {
	delete(c, id)
}

func (c concreteQueryRepository) put(q *QueryControl) error {
	if q.ID() == ID(uuid.Nil) {
		return errors.New("invalid query id")
	}

	if _, ok := c.Get(q.ID()); ok {
		return errors.New("query already exists")
	}

	c[q.ID()] = q

	return nil
}

func (c concreteQueryRepository) List() map[ID]*QueryControl {
	return c
}

var (
	queryRepository Repository
)

func QueryRepository() Repository {
	return queryRepository
}

func init() {
	queryRepository = concreteQueryRepository{}
}
