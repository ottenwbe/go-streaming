package processing

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

var (
	ErrInvalidQueryID     = errors.New("query: invalid query ID")
	ErrQueryAlreadyExists = errors.New("query: query with this ID already exists")
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

type concreteQueryRepository struct {
	queries map[ID]ContinuousQuery
	mutex   sync.RWMutex
}

func (c *concreteQueryRepository) String() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	s := "ContinuousQueryRepository {"
	for id := range c.queries {
		s = fmt.Sprintf("%v %v, \n", s, id)
	}
	s += "}"
	return s
}

func (c *concreteQueryRepository) Get(id ID) (q ContinuousQuery, ok bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	q, ok = c.queries[id]
	return
}

func (c *concreteQueryRepository) remove(id ID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.queries, id)
}

func (c *concreteQueryRepository) put(q ContinuousQuery) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if q.ID() == ID(uuid.Nil) {
		return ErrInvalidQueryID
	}
	if _, ok := c.queries[q.ID()]; ok {
		return ErrQueryAlreadyExists
	}

	c.queries[q.ID()] = q

	return nil
}

func (c *concreteQueryRepository) List() map[ID]ContinuousQuery {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	res := make(map[ID]ContinuousQuery, len(c.queries))
	for k, v := range c.queries {
		res[k] = v
	}
	return res
}

var (
	queryRepository Repository
)

// QueryRepository returns the singleton instance of the query repository.
func QueryRepository() Repository {
	return queryRepository
}

func init() {
	queryRepository = &concreteQueryRepository{
		queries: make(map[ID]ContinuousQuery),
	}
}
