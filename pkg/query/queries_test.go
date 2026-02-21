package query_test

import (
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/internal/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/query"
)

var _ = Describe("Continuous Query", func() {

	var (
		q     query.ContinuousQuery
		err   error
		count atomic.Int32
	)

	BeforeEach(func() {
		count = atomic.Int32{}
		q, err =
			query.Query[int](
				query.Process[int](
					engine.ContinuousSmaller[int](5),
					query.FromSourceStream[int]("test"),
				),
			)
		Expect(err).To(BeNil())
		err = q.Subscribe(
			func(event events.Event[int]) {
				count.Add(1)
			})
		Expect(err).To(BeNil())
		q.Run()
	})

	AfterEach(func() {
		query.Close(q)
	})

	Context("Query filtering all events < 5", func() {
		It("sorts out events >= 5", func() {
			pubsub.InstantPublishByTopic("test", 2)
			pubsub.InstantPublishByTopic("test", 12)
			pubsub.InstantPublishByTopic("test", 5)
			pubsub.InstantPublishByTopic("test", 29)
			pubsub.InstantPublishByTopic("test", 4)

			Eventually(count.Load).To(Equal(int32(2)))
		})
	})
})
