package processing_test

import (
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	query "github.com/ottenwbe/go-streaming/pkg/processing"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var _ = Describe("Builder API", func() {

	var (
		q   query.ContinuousQuery
		err error
	)

	AfterEach(func() {
		query.Close(q)
		q = nil
		err = nil
	})

	Context("Simple Pipeline", func() {
		It("should build and run a linear pipeline", func() {
			// Source -> Greater(10) -> Output
			b := query.NewBuilder[int]()
			b.From(query.Source[int]("builder-source-1")).
				ConnectTo(query.Operator[int]( //nolint:staticcheck
					query.Greater[int](10),
				))

			q, err = b.Build(true)
			Expect(err).To(BeNil())
			Expect(q).NotTo(BeNil())

			var received int
			var wg sync.WaitGroup

			wg.Add(1)
			err = q.Subscribe(func(e events.Event[int]) {
				received = e.GetContent()
				wg.Done()
			})
			Expect(err).To(BeNil())

			// Publish events
			err = pubsub.InstantPublishByTopic("builder-source-1", 5)
			Expect(err).To(BeNil())
			err = pubsub.InstantPublishByTopic("builder-source-1", 15)
			Expect(err).To(BeNil())

			// Wait for result
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			Eventually(done, 2*time.Second).Should(BeClosed())
			Expect(received).To(Equal(15))
		})
	})

	Context("Error Handling", func() {
		It("should fail if no input stream is defined", func() {
			b := query.NewBuilder[int]()
			// No From() called

			// ConnectTo requires input
			b.ConnectTo(query.Operator[int](query.Greater[int](10)))

			_, err := b.Build(false)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(query.ErrEmptyInput))
		})

		It("should fail if output is undefined (no streams)", func() {
			b := query.NewBuilder[int]()
			_, err := b.Build(false)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(query.ErrOutputUndefined))
		})
	})

	Context("Merge", func() {
		It("should correctly merge source streams from another builder", func() {
			// Builder 1 with one source
			b1 := query.NewBuilder[int]()
			b1.From(query.Source[int]("source1"))

			// Builder 2 with another source
			b2 := query.NewBuilder[int]()
			b2.From(query.Source[int]("source2"))

			// Merge b2 into b1
			b1.Merge(b2)

			// Add a process step to have a valid output
			fanIn := func(in []pubsub.StreamID, out []pubsub.StreamID, id query.OperatorID) (query.OperatorID, error) {
				policy := events.SelectionPolicyConfig{
					Type:         events.TemporalWindow,
					WindowLength: time.Second,
					WindowShift:  time.Second,
					WindowStart:  time.Now(),
				}
				config := query.MakeOperatorConfig(
					query.FANIN_OPERATOR,
					query.WithInput(query.MakeInputConfigs(in, policy)...),
					query.WithOutput(out...),
				)
				op := func(inputs map[int][]events.Event[int]) []int {
					return []int{}
				}
				return query.NewFanInOperator[int, int](config, op, id)
			}
			b1.ConnectTo(query.Operator[int](fanIn))

			// Build the query
			q, err = b1.Build(false)
			Expect(err).To(BeNil())
			Expect(q).NotTo(BeNil())

			// Verify that both source streams are registered as sources in the merged query
			p1, err1 := query.RegisterPublisher[int](q, "source1")
			query.UnRegisterPublisher(q, p1)
			Expect(err1).To(BeNil())

			p2, err2 := query.RegisterPublisher[int](q, "source2")
			query.UnRegisterPublisher(q, p2)
			Expect(err2).To(BeNil())

			// Verify that a non-source stream is not considered a source
			_, err3 := query.RegisterPublisher[int](q, "not-a-source")
			Expect(err3).To(HaveOccurred())
			Expect(err3.Error()).To(ContainSubstring("not found in query sources"))
		})
	})

	Context("Complex Topologies", func() {
		It("should support chaining multiple operators", func() {
			// Source -> Map (+1) -> Map (*2) -> Output
			// Input: 10 -> 11 -> 22
			b := query.NewBuilder[int]()
			b.From(query.Source[int]("chain-source"))

			// Op 1: Add 1
			b.ConnectTo(query.Operator[int](query.Map(func(e events.Event[int]) int {
				return e.GetContent() + 1
			})))

			// Op 2: Multiply by 2
			b.ConnectTo(query.Operator[int](query.Map(func(e events.Event[int]) int {
				return e.GetContent() * 2
			})))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var result int
			var wg sync.WaitGroup
			wg.Add(1)
			_ = q.Subscribe(func(e events.Event[int]) {
				result = e.GetContent()
				wg.Done()
			})

			_ = pubsub.InstantPublishByTopic("chain-source", 10)

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			Eventually(done).Should(BeClosed())

			Expect(result).To(Equal(22))
		})

		It("should detect ambiguous output when multiple streams remain", func() {
			b := query.NewBuilder[int]()
			b.From(query.Source[int]("fanout-source"))

			// Create a dummy fan-out operator that produces 2 output streams
			// We don't need a real implementation because Build() fails before execution
			noopFanOut := func(in []pubsub.StreamID, out []pubsub.StreamID, id query.OperatorID) (query.OperatorID, error) {
				return query.NilOperatorID(), nil
			}

			// Use CreateFanOutStream to generate multiple outputs
			b.ConnectTo(query.CreateFanOutStream[int](noopFanOut, 2))

			// Build should fail because we haven't converged the 2 streams into 1
			_, err := b.Build(false)
			Expect(err).To(MatchError(query.ErrAmbiguousOutput))
		})
	})

	Context("Fan-out -> Fan-in", func() {
		It("should support fan-out -> fan-in topologies", func() {
			b := query.NewBuilder[string]()
			b.From(query.Source[string]("fanout-fanin-source"))

			// 1. Fan-out: A simple map operator that will broadcast to two streams.
			splitter := query.Map(func(e events.Event[string]) string {
				return e.GetContent()
			})
			b.ConnectTo(query.CreateFanOutStream[string](splitter, 2))

			// 2. Fan-in: A fan-in operator that merges the two streams.
			baseTime := time.Now()
			mergerPolicy := events.SelectionPolicyConfig{
				Type:         events.TemporalWindow,
				WindowStart:  baseTime,
				WindowLength: time.Second,
				WindowShift:  time.Second,
			}

			merger := func(in []pubsub.StreamID, out []pubsub.StreamID, id query.OperatorID) (query.OperatorID, error) {
				config := query.MakeOperatorConfig(
					query.FANIN_OPERATOR,
					query.WithInput(query.MakeInputConfigs(in, mergerPolicy)...),
					query.WithOutput(out...),
				)
				op := func(inputs map[int][]events.Event[string]) []string {
					var allContents []string
					for _, streamEvents := range inputs {
						for _, ev := range streamEvents {
							allContents = append(allContents, ev.GetContent())
						}
					}
					return []string{strings.Join(allContents, "+")}
				}
				return query.NewFanInOperator[string, string](config, op, id)
			}
			b.ConnectTo(query.Operator[string](merger))

			q, err = b.Build(true)
			Expect(err).To(BeNil())

			var result string
			var mu sync.Mutex
			_ = q.Subscribe(func(e events.Event[string]) {
				mu.Lock()
				defer mu.Unlock()
				result = e.GetContent()
			})

			p, err := query.RegisterPublisher[string](q, "fanout-fanin-source")
			Expect(err).To(BeNil())
			defer query.UnRegisterPublisher(q, p)

			_ = p.Publish(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: baseTime.Add(100 * time.Millisecond)}, Content: "A"})
			_ = p.Publish(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: baseTime.Add(1100 * time.Millisecond)}, Content: "B"}) // Trigger window

			Eventually(func() string {
				mu.Lock()
				defer mu.Unlock()
				return result
			}).Should(Equal("A+A"))
		})
	})
})
