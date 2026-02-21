package query

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ID uniquely identifies a query in the repository.
type ID uuid.UUID

// String returns the string representation of the ID.
func (id ID) String() string {
	return uuid.UUID(id).String()
}

type (
	// Repository manages the storage and retrieval of ContinuousQueries.
	Repository interface {
		fmt.Stringer
		Get(id ID) (ContinuousQuery, bool)
		put(q ContinuousQuery) error
		remove(id ID)
		List() map[ID]ContinuousQuery
	}
)

type concreteQueryRepository map[ID]ContinuousQuery

func (c concreteQueryRepository) String() string {

	s := "ContinuousQueryRepository {"
	for id, _ := range c.List() {
		s = fmt.Sprintf("%v %v, \n", s, id)
	}
	s += "}"
	return s
}

func (c concreteQueryRepository) Get(id ID) (q ContinuousQuery, ok bool) {
	q, ok = c[id]
	return
}

func (c concreteQueryRepository) remove(id ID) {
	delete(c, id)
}

func (c concreteQueryRepository) put(q ContinuousQuery) error {
	if q.ID() == ID(uuid.Nil) {
		return errors.New("invalid query ID")
	}
	if _, ok := c.Get(q.ID()); ok {
		return errors.New("query with this ID already exists")
	}

	c[q.ID()] = q

	return nil
}

func (c concreteQueryRepository) List() map[ID]ContinuousQuery {
	return c
}

var (
	queryRepository Repository
)

// QueryRepository returns the singleton instance of the query repository.
func QueryRepository() Repository {
	return queryRepository
}

func init() {
	queryRepository = concreteQueryRepository{}
}
