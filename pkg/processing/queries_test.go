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
		b := query.NewBuilder[int]()
		b.From(query.Source[int]("test")).
			Process(query.Operator[int](query.Smaller[int](5)))
		q, err = b.Build(false)
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
			b := query.NewBuilder[int]()
			b.From(query.Source[int](topic))
			q, err := b.Build(false)
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
			b1 := query.NewBuilder[int](query.WithRepository(repo1))
			b1.From(query.Source[int](topic))
			q1, err1 := b1.Build(false)
			Expect(err1).To(BeNil())
			defer func() { _ = query.Close(q1) }()

			b2 := query.NewBuilder[int](query.WithRepository(repo2))
			b2.From(query.Source[int](topic))
			q2, err2 := b2.Build(false)
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
			b := query.NewBuilder[int](query.WithNewRepository())
			b.From(query.Source[int](topic))
			q, err := b.Build(false)
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

			b := query.NewBuilder[int]()
			b.From(query.Source[int]("topic")).
				Process(query.Operator[int](errOp))
			_, err := b.Build(false)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("op failed"))
		})

	})

	Describe("Default Operators", func() {
		Context("SelectFromMap", func() {
			It("should select a key from a map and forward its value", func() {
				topic := "map-select-topic"
				b := query.NewBuilder[any]()
				b.From(query.Source[map[string]any](topic)).
					Process(query.Operator[any](query.SelectFromMap("city")))
				q, err := b.Build(false)
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
				b := query.NewBuilder[any]()
				b.From(query.Source[map[string]any](topic)).
					Process(query.Operator[any](query.SelectFromMap("city")))
				q, err := b.Build(false)
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

				b := query.NewBuilder[string]()
				b.From(query.Source[int](topic)).
					Process(query.Operator[string](query.Map(mapper)))
				q, err := b.Build(false)
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
