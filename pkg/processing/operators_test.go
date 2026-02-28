package processing_test

import (
	"sync"
	"time"

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

		d := processing.MakeOperatorConfig(processing.PIPELINE_OPERATOR,
			processing.WithOutput(sidout),
			processing.WithAutoStart(true),
			processing.WithInput(processing.InputConfig{
				Stream: sidin,
				InputPolicy: events.SelectionPolicyConfig{
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

	Describe("Error Handling", func() {
		It("removes the operator from repository if AutoStart fails", func() {
			topic := "int-stream-fail-test"
			intStreamID, _ := pubsub.GetOrAddStream[int](topic)

			// Try to create an operator that treats int as string
			// This should fail at subscription time inside Start() due to type mismatch
			conf := processing.MakeOperatorConfig(processing.PIPELINE_OPERATOR,
				processing.WithInput(processing.InputConfig{
					Stream:      intStreamID, // Mismatch T
					InputPolicy: events.SelectionPolicyConfig{Type: events.SelectNext},
				}),
				processing.WithOutput(pubsub.MakeStreamID[string]("out-fail-test")),
				processing.WithAutoStart(true),
			)

			opFunc := func(in []events.Event[string]) []string { return nil }

			oid, err := processing.NewOperator[string, string](opFunc, conf, processing.NilOperatorID())
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(pubsub.ErrStreamTypeMismatch))

			_, exists := processing.OperatorRepository().Get(oid)
			Expect(exists).To(BeFalse())
		})

		It("cleans up publishers if subscription fails during Start", func() {
			topicIn := "int-stream-cleanup-test"
			topicOut := "out-stream-cleanup-test"
			intStreamID, _ := pubsub.GetOrAddStream[int](topicIn)

			conf := processing.MakeOperatorConfig(processing.PIPELINE_OPERATOR,
				processing.WithInput(processing.InputConfig{
					Stream:      intStreamID, // Mismatch
					InputPolicy: events.SelectionPolicyConfig{Type: events.SelectNext},
				}),
				processing.WithOutput(pubsub.MakeStreamID[string](topicOut)),
				processing.WithAutoStart(false),
			)

			opFunc := func(in []events.Event[string]) []string { return nil }
			oid, _ := processing.NewOperator[string, string](opFunc, conf, processing.NilOperatorID())
			op, _ := processing.OperatorRepository().Get(oid)

			err := op.Start()
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(pubsub.ErrStreamTypeMismatch))

			// Verify cleanup by checking if the output stream can be removed (implies no active publishers)
			pubsub.TryRemoveStreams(pubsub.MakeStreamID[string](topicOut))
			_, err = pubsub.GetConfiguration(pubsub.MakeStreamID[string](topicOut))
			Expect(err).To(Equal(pubsub.ErrStreamNotFound))
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
		d := processing.MakeOperatorConfig(processing.FILTER_OPERATOR,
			processing.WithOutput(sidout),
			processing.WithAutoStart(true),
			processing.WithInput(processing.InputConfig{
				Stream: sidin,
				InputPolicy: events.SelectionPolicyConfig{
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
		d := processing.MakeOperatorConfig(processing.MAP_OPERATOR,
			processing.WithOutput(sidout),
			processing.WithAutoStart(true),
			processing.WithInput(processing.InputConfig{
				Stream: sidin,
			}))

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

		Expect(receivedEvents[0].GetContent()).To(Equal(20))
		Expect(receivedEvents[1].GetContent()).To(Equal(40))
	})
})

var _ = Describe("FanInOperatorEngine", func() {
	var (
		oid            processing.OperatorID
		err            error
		sidin1, sidin2 pubsub.StreamID
		sidout         pubsub.StreamID
		baseTime       time.Time
	)

	BeforeEach(func() {
		sidin1, _ = pubsub.GetOrAddStream[int]("in-join-1")
		sidin2, _ = pubsub.GetOrAddStream[int]("in-join-2")
		sidout, _ = pubsub.GetOrAddStream[int]("out-join")

		baseTime = time.Now()

		policy := events.SelectionPolicyConfig{
			Type:         events.TemporalWindow,
			WindowStart:  baseTime,
			WindowLength: time.Second,
			WindowShift:  time.Second,
		}

		d := processing.MakeOperatorConfig(processing.FANIN_OPERATOR,
			processing.WithOutput(sidout),
			processing.WithAutoStart(true),
			processing.WithInput(
				processing.InputConfig{Stream: sidin1, InputPolicy: policy},
				processing.InputConfig{Stream: sidin2, InputPolicy: policy},
			),
		)

		fanInOp := func(inputs map[int][]events.Event[int]) []int {
			sum := 0
			for _, evs := range inputs {
				for _, e := range evs {
					sum += e.GetContent()
				}
			}
			return []int{sum}
		}

		oid, err = processing.NewOperator[int, int](fanInOp, d, processing.NilOperatorID())
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		processing.RemoveOperator(oid)
		pubsub.ForceRemoveStream(sidin1, sidin2, sidout)
	})

	It("should join events from multiple streams based on time window", func() {
		var (
			receivedEvents []events.Event[int]
			mutex          sync.Mutex
		)

		sub, err := pubsub.SubscribeByTopic[int]("out-join", func(event events.Event[int]) {
			mutex.Lock()
			defer mutex.Unlock()
			receivedEvents = append(receivedEvents, event)
		})
		Expect(err).To(BeNil())
		defer pubsub.Unsubscribe(sub)

		p1, _ := pubsub.RegisterPublisher[int](sidin1)
		p2, _ := pubsub.RegisterPublisher[int](sidin2)
		defer pubsub.UnRegisterPublisher(p1)
		defer pubsub.UnRegisterPublisher(p2)

		// Window 1: [baseTime, baseTime + 1s)
		e1 := &events.TemporalEvent[int]{
			Stamp:   events.TimeStamp{StartTime: baseTime.Add(100 * time.Millisecond)},
			Content: 1,
		}
		e2 := &events.TemporalEvent[int]{
			Stamp:   events.TimeStamp{StartTime: baseTime.Add(200 * time.Millisecond)},
			Content: 2,
		}

		p1.Publish(e1)
		p2.Publish(e2)

		// Trigger Window 1 by sending events in Window 2 (after baseTime + 1s)
		trigger1 := &events.TemporalEvent[int]{
			Stamp:   events.TimeStamp{StartTime: baseTime.Add(1500 * time.Millisecond)},
			Content: 10,
		}
		trigger2 := &events.TemporalEvent[int]{
			Stamp:   events.TimeStamp{StartTime: baseTime.Add(1500 * time.Millisecond)},
			Content: 20,
		}

		p1.Publish(trigger1)
		p2.Publish(trigger2)

		Eventually(func() int {
			mutex.Lock()
			defer mutex.Unlock()
			return len(receivedEvents)
		}).Should(Equal(1))

		mutex.Lock()
		defer mutex.Unlock()
		Expect(receivedEvents[0].GetContent()).To(Equal(3))
	})
})
