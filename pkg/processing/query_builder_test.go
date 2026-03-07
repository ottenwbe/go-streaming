package processing_test

import (
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
				Process(query.Operator[int]( //nolint:staticcheck
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

			// Process requires input
			b.Process(query.Operator[int](query.Greater[int](10)))

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
			// We need a FanIn operator because we have 2 input streams
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
				return query.NewOperator[int, int](op, config, id)
			}
			b1.Process(query.Operator[int](fanIn))

			// Build the query
			q, err = b1.Build(false)
			Expect(err).To(BeNil())
			Expect(q).NotTo(BeNil())

			// Verify that both source streams are registered as sources in the merged query
			_, err1 := query.RegisterPublisher[int](q, "source1")
			Expect(err1).To(BeNil())

			_, err2 := query.RegisterPublisher[int](q, "source2")
			Expect(err2).To(BeNil())

			// Verify that a non-source stream is not considered a source
			_, err3 := query.RegisterPublisher[int](q, "not-a-source")
			Expect(err3).To(HaveOccurred())
			Expect(err3.Error()).To(ContainSubstring("not found in query sources"))
		})
	})
})
