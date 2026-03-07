package processing_test

import (
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/processing"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var _ = Describe("Default Operators", func() {

	var (
		q   processing.ContinuousQuery
		err error
	)

	AfterEach(func() {
		if q != nil {
			_ = processing.Close(q)
		}
	})

	Context("Filter", func() {
		It("should filter events based on a predicate", func() {
			topic := "filter-test-topic"
			b := processing.NewBuilder[int]()
			b.From(processing.Source[int](topic)).
				Process(processing.Operator[int](processing.Filter(func(e events.Event[int]) bool {
					return e.GetContent()%2 == 0
				})))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var results []int
			var wg sync.WaitGroup
			wg.Add(2) // Expecting 2 even numbers

			err = q.Subscribe(func(e events.Event[int]) {
				results = append(results, e.GetContent())
				wg.Done()
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, 1)
			pubsub.InstantPublishByTopic(topic, 2)
			pubsub.InstantPublishByTopic(topic, 3)
			pubsub.InstantPublishByTopic(topic, 4)

			wg.Wait()
			Expect(results).To(Equal([]int{2, 4}))
		})
	})

	Context("Even / Odd", func() {
		It("should filter even numbers", func() {
			topic := "even-test-topic"
			b := processing.NewBuilder[int]()
			b.From(processing.Source[int](topic)).
				Process(processing.Operator[int](processing.Even[int]()))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var results []int
			var wg sync.WaitGroup
			wg.Add(2)

			err = q.Subscribe(func(e events.Event[int]) {
				results = append(results, e.GetContent())
				wg.Done()
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, 1)
			pubsub.InstantPublishByTopic(topic, 2)
			pubsub.InstantPublishByTopic(topic, 3)
			pubsub.InstantPublishByTopic(topic, 4)

			wg.Wait()
			Expect(results).To(Equal([]int{2, 4}))
		})

		It("should filter odd numbers", func() {
			topic := "odd-test-topic"
			b := processing.NewBuilder[int]()
			b.From(processing.Source[int](topic)).
				Process(processing.Operator[int](processing.Odd[int]()))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var results []int
			var wg sync.WaitGroup
			wg.Add(2)

			err = q.Subscribe(func(e events.Event[int]) {
				results = append(results, e.GetContent())
				wg.Done()
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, 1)
			pubsub.InstantPublishByTopic(topic, 2)
			pubsub.InstantPublishByTopic(topic, 3)
			pubsub.InstantPublishByTopic(topic, 4)

			wg.Wait()
			Expect(results).To(Equal([]int{1, 3}))
		})

		It("should filter 'even' numbers from floats by truncating", func() {
			topic := "even-float-test-topic"
			b := processing.NewBuilder[float64]()
			b.From(processing.Source[float64](topic)).
				Process(processing.Operator[float64](processing.Even[float64]()))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var results []float64
			var wg sync.WaitGroup
			wg.Add(2) // Expecting 2.1 and 4.9

			err = q.Subscribe(func(e events.Event[float64]) {
				results = append(results, e.GetContent())
				wg.Done()
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, 1.5) // int(1.5) is 1 (odd)
			pubsub.InstantPublishByTopic(topic, 2.1) // int(2.1) is 2 (even)
			pubsub.InstantPublishByTopic(topic, 3.9) // int(3.9) is 3 (odd)
			pubsub.InstantPublishByTopic(topic, 4.9) // int(4.9) is 4 (even)

			wg.Wait()
			Expect(results).To(ConsistOf(2.1, 4.9))
		})
	})

	Context("Limit", func() {
		It("should only take the first N events", func() {
			topic := "limit-test-topic"
			b := processing.NewBuilder[int]()
			b.From(processing.Source[int](topic)).
				Process(processing.Operator[int](processing.Limit[int](2)))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var results []int
			var mu sync.Mutex

			err = q.Subscribe(func(e events.Event[int]) {
				mu.Lock()
				defer mu.Unlock()
				results = append(results, e.GetContent())
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, 1)
			pubsub.InstantPublishByTopic(topic, 2)
			pubsub.InstantPublishByTopic(topic, 3)
			pubsub.InstantPublishByTopic(topic, 4)

			Eventually(func() []int {
				mu.Lock()
				defer mu.Unlock()
				return results
			}).Should(Equal([]int{1, 2}))

			Consistently(func() []int {
				mu.Lock()
				defer mu.Unlock()
				return results
			}).Should(Equal([]int{1, 2}))
		})
	})

	Context("FlatMap", func() {
		It("should map one event to multiple events", func() {
			topic := "flatmap-test-topic"
			b := processing.NewBuilder[string]()
			b.From(processing.Source[string](topic)).
				Process(processing.Operator[string](processing.FlatMap(func(e events.Event[string]) []string {
					return strings.Split(e.GetContent(), " ")
				})))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var results []string
			var wg sync.WaitGroup
			wg.Add(4) // "hello", "world", "foo", "bar"

			err = q.Subscribe(func(e events.Event[string]) {
				results = append(results, e.GetContent())
				wg.Done()
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, "hello world")
			pubsub.InstantPublishByTopic(topic, "foo bar")

			wg.Wait()
			Expect(results).To(Equal([]string{"hello", "world", "foo", "bar"}))
		})

		It("should handle empty results (filtering)", func() {
			topic := "flatmap-empty-test-topic"
			b := processing.NewBuilder[int]()
			b.From(processing.Source[int](topic)).
				Process(processing.Operator[int](processing.FlatMap(func(e events.Event[int]) []int {
					if e.GetContent() > 5 {
						return []int{e.GetContent()}
					}
					return []int{}
				})))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var results []int
			var wg sync.WaitGroup
			wg.Add(1)

			err = q.Subscribe(func(e events.Event[int]) {
				results = append(results, e.GetContent())
				wg.Done()
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, 1)
			pubsub.InstantPublishByTopic(topic, 10)

			wg.Wait()
			Expect(results).To(Equal([]int{10}))
		})
	})

	Context("Observe", func() {
		It("should execute side effect and pass event through", func() {
			topic := "observe-test-topic"
			var observedValue int

			wg := sync.WaitGroup{}
			wg.Add(1)

			b := processing.NewBuilder[int]().
				From(processing.Source[int](topic)).
				Process(processing.Operator[int](processing.Observe(func(e events.Event[int]) {
					observedValue = e.GetContent()
					wg.Done()
				})))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var outputValue int
			var wgOutput sync.WaitGroup
			wgOutput.Add(1)

			err =
				q.Subscribe(func(e events.Event[int]) {
					outputValue = e.GetContent()
					wgOutput.Done()
				})

			pubsub.InstantPublishByTopic(topic, 42)

			wg.Wait()
			wgOutput.Wait()

			Expect(observedValue).To(Equal(42))
			Expect(outputValue).To(Equal(42))
		})
	})

	Context("SelectFromMap", func() {
		It("should select a key from a map and forward its value", func() {
			topic := "map-select-topic"
			b := processing.NewBuilder[any]()
			b.From(processing.Source[map[string]any](topic)).
				Process(processing.Operator[any](processing.SelectFromMap("city")))
			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var receivedValue any
			var wg sync.WaitGroup
			wg.Add(1)

			err = q.Subscribe(func(e events.Event[any]) {
				receivedValue = e.GetContent()
				wg.Done()
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, map[string]any{"name": "a", "city": "town"})

			wg.Wait()
			Expect(receivedValue).To(Equal("town"))
		})

		It("should forward nil if key is not found", func() {
			topic := "map-select-fail-topic"
			b := processing.NewBuilder[any]()
			b.From(processing.Source[map[string]any](topic)).
				Process(processing.Operator[any](processing.SelectFromMap("city")))
			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var receivedValue any
			var wg sync.WaitGroup
			wg.Add(1)

			err = q.Subscribe(func(e events.Event[any]) {
				receivedValue = e.GetContent()
				wg.Done()
			})
			Expect(err).To(BeNil())

			// Publish map without "city" key
			pubsub.InstantPublishByTopic(topic, map[string]any{"name": "a", "country": "some"})

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

			b := processing.NewBuilder[string]()
			b.From(processing.Source[int](topic)).
				Process(processing.Operator[string](processing.Map(mapper)))
			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var results []string
			var wg sync.WaitGroup
			wg.Add(2)

			err = q.Subscribe(func(e events.Event[string]) {
				results = append(results, e.GetContent())
				wg.Done()
			})
			Expect(err).To(BeNil())

			pubsub.InstantPublishByTopic(topic, 1)
			pubsub.InstantPublishByTopic(topic, 2)

			wg.Wait()
			Expect(results).To(ContainElements("odd", "even"))
		})
	})

	Context("Join Operators", func() {
		var (
			baseTime  time.Time
			policy    events.SelectionPolicyConfig
			results   []events.Event[map[string]any]
			resultsMu sync.Mutex
		)

		BeforeEach(func() {
			baseTime = time.Now()
			policy = events.MakeSelectionPolicyByValue(events.TemporalWindow, 0, 0, baseTime, time.Second, time.Second)
			results = make([]events.Event[map[string]any], 0)
			resultsMu = sync.Mutex{}
		})

		publishTestEvents := func(entryTopic, exitTopic string) {
			entryPub, _ := pubsub.RegisterPublisherByTopic[map[string]any](entryTopic)
			defer pubsub.UnRegisterPublisher(entryPub)

			exitPub, _ := pubsub.RegisterPublisherByTopic[map[string]any](exitTopic)
			defer pubsub.UnRegisterPublisher(exitPub)

			// events matching window
			_ = entryPub.Publish(&events.TemporalEvent[map[string]any]{Stamp: events.TimeStamp{StartTime: baseTime.Add(100 * time.Millisecond)}, Content: map[string]any{"vehicle_id": 1, "entry_loc": "A"}})
			_ = exitPub.Publish(&events.TemporalEvent[map[string]any]{Stamp: events.TimeStamp{StartTime: baseTime.Add(200 * time.Millisecond)}, Content: map[string]any{"vehicle_id": 1, "exit_loc": "B"}})
			_ = entryPub.Publish(&events.TemporalEvent[map[string]any]{Stamp: events.TimeStamp{StartTime: baseTime.Add(300 * time.Millisecond)}, Content: map[string]any{"vehicle_id": 2, "entry_loc": "A"}})

			// trigger the window shift
			_ = entryPub.Publish(&events.TemporalEvent[map[string]any]{Stamp: events.TimeStamp{StartTime: baseTime.Add(1100 * time.Millisecond)}, Content: map[string]any{"vehicle_id": 3, "entry_loc": "A"}})
			_ = exitPub.Publish(&events.TemporalEvent[map[string]any]{Stamp: events.TimeStamp{StartTime: baseTime.Add(1200 * time.Millisecond)}, Content: map[string]any{"vehicle_id": 99, "exit_loc": "B"}})
		}

		It("should perform an inner join on two map streams", func() {
			b := processing.NewBuilder[map[string]any]()
			b.From(processing.Source[map[string]any]("entry_cam")).
				From(processing.Source[map[string]any]("exit_cam")).
				Process(processing.Operator[map[string]any](processing.Join("vehicle_id", policy)))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			q.Subscribe(func(e events.Event[map[string]any]) {
				resultsMu.Lock()
				defer resultsMu.Unlock()
				results = append(results, e)
			})

			publishTestEvents("entry_cam", "exit_cam")

			Eventually(func() []events.Event[map[string]any] {
				resultsMu.Lock()
				defer resultsMu.Unlock()
				return results
			}).Should(HaveLen(1))

			resultsMu.Lock()
			res := results[0]
			resultsMu.Unlock()
			Expect(res.GetContent()).To(SatisfyAll(HaveKeyWithValue("vehicle_id", 1), HaveKeyWithValue("entry_loc", "A"), HaveKeyWithValue("exit_loc", "B")))
		})

		It("should perform a left join on two map streams", func() {
			b := processing.NewBuilder[map[string]any]()
			b.From(processing.Source[map[string]any]("entry_cam_left")).
				From(processing.Source[map[string]any]("exit_cam_left")).
				Process(processing.Operator[map[string]any](processing.LeftJoin("vehicle_id", policy)))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			q.Subscribe(func(e events.Event[map[string]any]) {
				resultsMu.Lock()
				defer resultsMu.Unlock()
				results = append(results, e)
			})

			publishTestEvents("entry_cam_left", "exit_cam_left")

			Eventually(func() []events.Event[map[string]any] {
				resultsMu.Lock()
				defer resultsMu.Unlock()
				return results
			}).Should(HaveLen(2))
		})
	})
})
