package processing_test

import (
	"errors"
	"sync"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	query "github.com/ottenwbe/go-streaming/pkg/processing"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
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
					query.Smaller[int](5),
					query.FromSourceStream[int]("test"),
				),
			)
		Expect(err).To(BeNil())
		err = q.Subscribe(
			func(event events.Event[int]) {
				count.Add(1)
			})
		Expect(err).To(BeNil())
		err = q.Run()
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		_ = query.Close(q)
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

	Describe("Query Lifecycle", func() {
		It("stops processing events after Close() is called", func() {
			topic := "lifecycle-test"
			q, err := query.Query[int](query.FromSourceStream[int](topic))
			Expect(err).To(BeNil())

			var count atomic.Int32
			q.Subscribe(func(e events.Event[int]) { count.Add(1) })
			q.Run()

			pubsub.InstantPublishByTopic(topic, 1)
			Eventually(count.Load).Should(Equal(int32(1)))

			_ = query.Close(q)
			pubsub.InstantPublishByTopic(topic, 2)
			Consistently(count.Load).Should(Equal(int32(1)))
		})
	})

	Describe("Query Options & Isolation", func() {
		It("supports isolated repositories via WithRepository", func() {
			repo1 := pubsub.NewStreamRepository()
			repo2 := pubsub.NewStreamRepository()
			topic := "isolated-topic"

			// Create two queries on different repositories listening to the same topic name
			q1, err1 := query.Query[int](
				query.FromSourceStream[int](topic),
				query.WithRepository(repo1),
			)
			Expect(err1).To(BeNil())
			defer func() { _ = query.Close(q1) }()

			q2, err2 := query.Query[int](
				query.FromSourceStream[int](topic),
				query.WithRepository(repo2),
			)
			Expect(err2).To(BeNil())
			defer func() { _ = query.Close(q2) }()

			// Subscribe to both
			var received1, received2 atomic.Int32
			_ = q1.Subscribe(func(e events.Event[int]) { received1.Add(1) })
			_ = q2.Subscribe(func(e events.Event[int]) { received2.Add(1) })

			err := q1.Run()
			Expect(err).To(BeNil())
			err = q2.Run()
			Expect(err).To(BeNil())

			// PublishContent to repo1 only
			err = pubsub.InstantPublishByTopicOnRepository(repo1, topic, 10)
			Expect(err).To(BeNil())

			// Verify isolation
			Eventually(received1.Load).Should(Equal(int32(1)))
			Consistently(received2.Load).Should(Equal(int32(0)))
		})

		It("WithNewRepository creates a private repository hidden from default", func() {
			topic := "private-topic"
			q, err := query.Query[int](
				query.FromSourceStream[int](topic),
				query.WithNewRepository(),
			)
			Expect(err).To(BeNil())
			defer func() { _ = query.Close(q) }()

			// The default repository should NOT have this stream
			_, err = pubsub.GetConfiguration(pubsub.MakeStreamID[int](topic))
			Expect(err).To(Equal(pubsub.ErrStreamNotFound))
		})
	})

	Describe("Error Handling", func() {
		It("Process propagates operator creation errors", func() {
			errOp := func(in []pubsub.StreamID, out []pubsub.StreamID, id query.OperatorID) (query.OperatorID, error) {
				return query.OperatorID{}, errors.New("op failed")
			}

			_, err := query.Query[int](
				query.Process[int](
					errOp,
					query.FromSourceStream[int]("topic"),
				),
			)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("op failed"))
		})

	})

	Describe("Default Operators", func() {
		Context("SelectFromMap", func() {
			It("should select a key from a map and forward its value", func() {
				topic := "map-select-topic"
				q, err := query.Query[any](
					query.Process[any](
						query.SelectFromMap("city"),
						query.FromSourceStream[map[string]any](topic),
					),
				)
				Expect(err).To(BeNil())

				var receivedValue any
				var wg sync.WaitGroup
				wg.Add(1)

				err = q.Subscribe(func(e events.Event[any]) {
					receivedValue = e.GetContent()
					wg.Done()
				})
				Expect(err).To(BeNil())
				err = q.Run()
				Expect(err).To(BeNil())
				defer query.Close(q)

				pubsub.InstantPublishByTopic(topic, map[string]any{"name": "John", "city": "New York"})

				wg.Wait()
				Expect(receivedValue).To(Equal("New York"))
			})

			It("should forward nil if key is not found", func() {
				topic := "map-select-fail-topic"
				q, err := query.Query[any](
					query.Process[any](
						query.SelectFromMap("city"),
						query.FromSourceStream[map[string]any](topic),
					),
				)
				Expect(err).To(BeNil())

				var receivedValue any
				var wg sync.WaitGroup
				wg.Add(1)

				err = q.Subscribe(func(e events.Event[any]) {
					receivedValue = e.GetContent()
					wg.Done()
				})
				Expect(err).To(BeNil())
				err = q.Run()
				Expect(err).To(BeNil())
				defer query.Close(q)

				// Publish map without "city" key
				pubsub.InstantPublishByTopic(topic, map[string]any{"name": "Jane", "country": "USA"})

				wg.Wait()
				Expect(receivedValue).To(BeNil())
			})
		})

		Context("Map", func() {
			It("should map events using a custom function", func() {
				topic := "generic-map-topic"
				mapper := func(e events.Event[int]) string {
					if e.GetContent()%2 == 0 {
						return "even"
					}
					return "odd"
				}

				q, err := query.Query[string](
					query.Process[string](
						query.Map(mapper),
						query.FromSourceStream[int](topic),
					),
				)
				Expect(err).To(BeNil())

				var results []string
				var wg sync.WaitGroup
				wg.Add(2)

				err = q.Subscribe(func(e events.Event[string]) {
					results = append(results, e.GetContent())
					wg.Done()
				})
				Expect(err).To(BeNil())
				err = q.Run()
				Expect(err).To(BeNil())
				defer query.Close(q)

				pubsub.InstantPublishByTopic(topic, 1)
				pubsub.InstantPublishByTopic(topic, 2)

				wg.Wait()
				Expect(results).To(ContainElements("odd", "even"))
			})
		})
	})
})
