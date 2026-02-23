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
				Process(query.Operator[int](
					query.ContinuousGreater[int](10),
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
			b.Process(query.Operator[int](query.ContinuousGreater[int](10)))

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
})
