package engine_test

import (
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/internal/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

func TestEngine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Engine Suite")
}

var _ = Describe("OperatorRepository", func() {

	var (
		oid engine.OperatorID
		err error
	)

	BeforeEach(func() {
		d := engine.NewOperatorDescription(engine.PIPELINE_OPERATOR,
			engine.WithOutput("out"),
			engine.WithInput(engine.InputDescription{
				Stream: "in",
				InputPolicy: selection.PolicyDescription{
					Active: true,
					Type:   selection.CountingWindow,
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

		oid, err = engine.NewOperator[int, int](
			sum,
			d,
		)
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		engine.RemoveOperator(oid)
	})

	Context("NewOperator", func() {
		It("adds new operators to the map and it can be retrieved", func() {
			operator, ok := engine.OperatorRepository().Get(oid)
			Expect(ok).To(BeTrue())
			Expect(operator).To(Not(BeNil()))
		})
	})
	Context("RemoveOperator", func() {
		It("ensures that an operator is no longer managed by a repository", func() {
			engine.RemoveOperator(oid)

			operator, ok := engine.OperatorRepository().Get(oid)
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
		oid engine.OperatorID
		err error
	)

	BeforeEach(func() {
		d := engine.NewOperatorDescription(engine.FILTER_OPERATOR,
			engine.WithOutput("out-filter"),
			engine.WithInput(engine.InputDescription{
				Stream: "in-filter",
				InputPolicy: selection.PolicyDescription{
					Type: selection.SelectNext,
				},
			}))

		isEven := func(event events.Event[int]) bool {
			return event.GetContent()%2 == 0
		}

		oid, err = engine.NewOperator[int, int](isEven, d)
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		engine.RemoveOperator(oid)
		pubsub.ForceRemoveStream(pubsub.MakeStreamID[int]("in-filter"), pubsub.MakeStreamID[int]("out-filter"))
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
		oid engine.OperatorID
		err error
	)

	BeforeEach(func() {
		d := engine.NewOperatorDescription(engine.MAP_OPERATOR,
			engine.WithOutput("out-map"),
			engine.WithInput(engine.InputDescription{
				Stream: "in-map",
				// InputPolicy is ignored for MapOperator
			}))

		// Map: multiply by 2
		double := func(event events.Event[int]) int {
			return event.GetContent() * 2
		}

		oid, err = engine.NewOperator[int, int](double, d)
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		engine.RemoveOperator(oid)
		pubsub.ForceRemoveStream(pubsub.MakeStreamID[int]("in-map"), pubsub.MakeStreamID[int]("out-map"))
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
