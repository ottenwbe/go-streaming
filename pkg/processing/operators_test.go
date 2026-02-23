package processing_test

import (
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/processing"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var _ = Describe("OperatorRepository", func() {

	var (
		sidin, sidout pubsub.StreamID
		oid           processing.OperatorID
		err           error
	)

	BeforeEach(func() {

		sidin, _ = pubsub.GetOrAddStream[int]("in")
		sidout, _ = pubsub.GetOrAddStream[int]("out")

		d := processing.NewOperatorDescription(processing.PIPELINE_OPERATOR,
			processing.WithOutput(sidout),
			processing.WithAutoStart(true),
			processing.WithInput(processing.InputDescription{
				Stream: sidin,
				InputPolicy: events.PolicyDescription{
					Active: true,
					Type:   events.CountingWindow,
					Size:   10,
					Slide:  10,
				},
			}))

		sum := func(input []events.Event[int]) []int {

			if len(input) == 0 {
				return []int{}
			}

			s := 0
			for _, e := range input {
				if e != nil {
					s += e.GetContent()
				}
			}
			return []int{s}
		}

		oid, err = processing.NewOperator[int, int](
			sum,
			d,
			processing.NilOperatorID(),
		)
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		processing.RemoveOperator(oid)
		pubsub.TryRemoveStreams(sidin, sidout)
	})

	Context("NewOperator", func() {
		It("adds new operators to the map and it can be retrieved", func() {
			operator, ok := processing.OperatorRepository().Get(oid)
			Expect(ok).To(BeTrue())
			Expect(operator).To(Not(BeNil()))
		})
	})
	Context("RemoveOperator", func() {
		It("ensures that an operator is no longer managed by a repository", func() {
			processing.RemoveOperator(oid)

			operator, ok := processing.OperatorRepository().Get(oid)
			Expect(ok).To(BeFalse())
			Expect(operator).To(BeNil())
		})
	})
	Context("PipelineOperatorEngine", func() {
		It("Processes Events in order", func() {
			var (
				e_in, e_out events.Event[int]
				mu          sync.Mutex
			)
			sub, _ := pubsub.SubscribeByTopic[int]("in", func(event events.Event[int]) {
				mu.Lock()
				defer mu.Unlock()
				e_in = event
			})
			sub2, _ := pubsub.SubscribeByTopic[int]("out", func(event events.Event[int]) {
				mu.Lock()
				defer mu.Unlock()
				e_out = event
			})
			defer pubsub.Unsubscribe(sub)
			defer pubsub.Unsubscribe(sub2)

			for i := 1; i <= 10; i++ {
				pubsub.InstantPublishByTopic[int]("in", i)
			}

			Eventually(func() events.Event[int] { mu.Lock(); defer mu.Unlock(); return e_in }).Should(Not(BeNil()))
			Eventually(func() events.Event[int] { mu.Lock(); defer mu.Unlock(); return e_out }).Should(Not(BeNil()))
			mu.Lock()
			defer mu.Unlock()
			Expect(e_out.GetContent()).To(Equal(55))
		})
	})
})

var _ = Describe("FilterOperatorEngine", func() {
	var (
		oid           processing.OperatorID
		err           error
		sidin, sidout pubsub.StreamID
	)

	BeforeEach(func() {
		sidin, _ = pubsub.GetOrAddStream[int]("in-filter")
		sidout, _ = pubsub.GetOrAddStream[int]("out-filter")
		d := processing.NewOperatorDescription(processing.FILTER_OPERATOR,
			processing.WithOutput(sidout),
			processing.WithAutoStart(true),
			processing.WithInput(processing.InputDescription{
				Stream: sidin,
				InputPolicy: events.PolicyDescription{
					Type: events.SelectNext,
				},
			}))

		isEven := func(event events.Event[int]) bool {
			return event.GetContent()%2 == 0
		}

		oid, err = processing.NewOperator[int, int](isEven, d, processing.NilOperatorID())
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		processing.RemoveOperator(oid)
		pubsub.ForceRemoveStream(sidin, sidout)
	})

	It("should only pass events that match the predicate", func() {
		var (
			receivedEvents []events.Event[int]
			mutex          sync.Mutex
		)

		sub, err := pubsub.SubscribeByTopic[int]("out-filter", func(event events.Event[int]) {
			mutex.Lock()
			defer mutex.Unlock()
			receivedEvents = append(receivedEvents, event)
		})
		Expect(err).To(BeNil())
		defer pubsub.Unsubscribe(sub)

		pubsub.InstantPublishByTopic[int]("in-filter", 1)
		pubsub.InstantPublishByTopic[int]("in-filter", 2)
		pubsub.InstantPublishByTopic[int]("in-filter", 3)
		pubsub.InstantPublishByTopic[int]("in-filter", 4)

		Eventually(func() int {
			mutex.Lock()
			defer mutex.Unlock()
			return len(receivedEvents)
		}).Should(Equal(2))

		mutex.Lock()
		capturedEvents := make([]events.Event[int], len(receivedEvents))
		copy(capturedEvents, receivedEvents)
		mutex.Unlock()

		Expect(capturedEvents[0].GetContent()).To(Equal(2))
		Expect(capturedEvents[1].GetContent()).To(Equal(4))
	})
})

var _ = Describe("MapOperatorEngine", func() {
	var (
		oid           processing.OperatorID
		err           error
		sidin, sidout pubsub.StreamID
	)

	BeforeEach(func() {
		sidin, _ = pubsub.GetOrAddStream[int]("in-map")
		sidout, _ = pubsub.GetOrAddStream[int]("out-map")
		d := processing.NewOperatorDescription(processing.MAP_OPERATOR,
			processing.WithOutput(sidout),
			processing.WithAutoStart(true),
			processing.WithInput(processing.InputDescription{
				Stream: sidin,
				// InputPolicy is ignored for MapOperator
			}))

		// Map: multiply by 2
		double := func(event events.Event[int]) int {
			return event.GetContent() * 2
		}

		oid, err = processing.NewOperator[int, int](double, d, processing.NilOperatorID())
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		processing.RemoveOperator(oid)
		pubsub.ForceRemoveStream(sidin, sidout)
	})

	It("should map events 1-to-1", func() {
		var (
			receivedEvents []events.Event[int]
			mutex          sync.Mutex
		)
		sub, err := pubsub.SubscribeByTopic[int]("out-map", func(event events.Event[int]) {
			mutex.Lock()
			defer mutex.Unlock()
			receivedEvents = append(receivedEvents, event)
		})
		Expect(err).To(BeNil())
		defer pubsub.Unsubscribe(sub)

		pubsub.InstantPublishByTopic[int]("in-map", 10)
		pubsub.InstantPublishByTopic[int]("in-map", 20)

		Eventually(func() int {
			mutex.Lock()
			defer mutex.Unlock()
			return len(receivedEvents)
		}).Should(Equal(2))

		mutex.Lock()
		capturedEvents := make([]events.Event[int], len(receivedEvents))
		copy(capturedEvents, receivedEvents)
		mutex.Unlock()

		Expect(capturedEvents[0].GetContent()).To(Equal(20))
		Expect(capturedEvents[1].GetContent()).To(Equal(40))
	})
})

var _ = Describe("FanOutOperatorEngine", func() {
	var (
		oid              processing.OperatorID
		err              error
		sidin            pubsub.StreamID
		sidout1, sidout2 pubsub.StreamID
	)

	BeforeEach(func() {
		sidin, _ = pubsub.GetOrAddStream[int]("in-fanout")
		sidout1, _ = pubsub.GetOrAddStream[int]("out-fanout-1")
		sidout2, _ = pubsub.GetOrAddStream[int]("out-fanout-2")
		d := processing.NewOperatorDescription(processing.FANOUT_OPERATOR,
			processing.WithOutput(sidout1, sidout2),
			processing.WithAutoStart(true),
			processing.WithInput(processing.InputDescription{
				Stream: sidin,
			}))

		oid, err = processing.NewOperator[int, int](nil, d, processing.NilOperatorID())
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		processing.RemoveOperator(oid)
		pubsub.ForceRemoveStream(sidin, sidout1, sidout2)
	})

	It("should broadcast events to all outputs", func() {
		var (
			received1, received2 int
			wg                   sync.WaitGroup
		)
		wg.Add(2)

		sub1, _ := pubsub.SubscribeByTopic[int]("out-fanout-1", func(event events.Event[int]) {
			received1 = event.GetContent()
			wg.Done()
		})
		sub2, _ := pubsub.SubscribeByTopic[int]("out-fanout-2", func(event events.Event[int]) {
			received2 = event.GetContent()
			wg.Done()
		})
		defer pubsub.Unsubscribe(sub1)
		defer pubsub.Unsubscribe(sub2)

		pubsub.InstantPublishByTopic[int]("in-fanout", 42)

		wg.Wait()

		Expect(received1).To(Equal(42))
		Expect(received2).To(Equal(42))
	})
})
