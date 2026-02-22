package pubsub_test

import (
	"fmt"
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stream", func() {
	Describe("localSyncStream", func() {
		var (
			streamID pubsub.StreamID
			topic    = "test"
		)

		BeforeEach(func() {
			var err error
			streamID, err = pubsub.AddOrReplaceStream[string](topic)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			pubsub.ForceRemoveStream(streamID)
		})

		Context("description", func() {
			It("should be retrievable", func() {
				retrievedDesc, err := pubsub.GetDescription(streamID)
				Expect(err).To(BeNil())
				Expect(retrievedDesc.ID).To(Equal(streamID))
			})
			It("should contain a valid id", func() {
				Expect(streamID.IsNil()).ToNot(BeTrue())
			})
		})

		Context("closing the stream", func() {
			It("without force ensures that the stream receiver is still functioning after trying to close the stream", func() {
				_, err := pubsub.SubscribeByTopicID[string](streamID, func(_ events.Event[string]) {})
				Expect(err).To(BeNil())

				pubsub.TryRemoveStreams(streamID)

				_, err = pubsub.GetDescription(streamID)
				Expect(err).To(BeNil()) // stream should still exist
			})
		})

		Context("published events", func() {
			It("should be received", func() {
				var eventResult events.Event[string]
				content := "test-1"
				done := make(chan bool)

				receiver, _ := pubsub.SubscribeByTopicID[string](streamID, func(event events.Event[string]) {
					eventResult = event
					done <- true
				})
				defer pubsub.Unsubscribe(receiver)

				p, _ := pubsub.RegisterPublisher[string](streamID)
				defer pubsub.UnRegisterPublisher[string](p)

				p.PublishContent(content)
				<-done

				Expect(eventResult.GetContent()).To(Equal(content))
			})
		})
	})

	Describe("localAsyncStream", func() {
		var (
			streamID pubsub.StreamID
			topic    = "test3"
		)

		BeforeEach(func() {
			var err error
			streamID, err = pubsub.AddOrReplaceStream[string](topic, pubsub.WithAsynchronousStream(true))
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			pubsub.ForceRemoveStream(streamID)
		})

		Context("description", func() {
			It("should be retrievable", func() {
				retrievedDesc, err := pubsub.GetDescription(streamID)
				Expect(err).To(BeNil())
				Expect(retrievedDesc.ID).To(Equal(streamID))
			})
			It("should contain a valid id", func() {
				Expect(streamID.IsNil()).ToNot(BeTrue())
			})
		})

		Context("closing the stream", func() {
			It("without force ensures that the stream receiver is still functioning after trying to close the stream", func() {
				_, err := pubsub.SubscribeByTopicID[string](streamID, func(_ events.Event[string]) {})
				Expect(err).To(BeNil())

				pubsub.TryRemoveStreams(streamID)

				_, err = pubsub.GetDescription(streamID)
				Expect(err).To(BeNil()) // stream should still exist
			})

			It("should be subscribable (auto-create) after closing the stream", func() {
				// close stream (it has no subscribers/publishers yet)
				pubsub.TryRemoveStreams(streamID)

				result, err := pubsub.SubscribeByTopicID[string](streamID, func(_ events.Event[string]) {})
				Expect(result).NotTo(BeNil())
				Expect(err).To(BeNil())
				pubsub.Unsubscribe(result)
			})
		})

		Context("copy one stream to another", func() {
			It("should not lose events", func() {

				numE := 300

				wg := sync.WaitGroup{}
				p, err := pubsub.RegisterPublisher[string](streamID)
				Expect(err).To(BeNil())
				defer pubsub.UnRegisterPublisher(p)

				receivedWg := sync.WaitGroup{}
				receivedWg.Add(numE)
				s, err := pubsub.SubscribeByTopicID[string](streamID, func(_ events.Event[string]) {
					receivedWg.Done()
				})
				Expect(err).To(BeNil())
				defer pubsub.Unsubscribe[string](s)

				wg.Go(func() {
					for i := range numE {
						p.PublishContent(fmt.Sprintf("a%v", i))

					}
				})

				streamID2, err := pubsub.AddOrReplaceStream[string](topic, pubsub.WithAsynchronousStream(false))
				Expect(err).To(BeNil())
				defer pubsub.TryRemoveStreams(streamID2)

				wg.Wait()
				receivedWg.Wait()
			})
		})

		Context("publishing and receiving events", func() {
			It("should not block", func() {

				eventResult := make([]events.Event[string], 3)
				content1 := "test-3-1"
				content2 := "test-3-2"
				content3 := "test-3-3"
				done := make(chan bool)
				count := 0

				receiver, _ := pubsub.SubscribeByTopicID[string](streamID, func(e events.Event[string]) {
					if count < 3 {
						eventResult[count] = e
						count++
						if count == 3 {
							done <- true
						}
					}
				})
				defer pubsub.Unsubscribe[string](receiver)

				publisher, _ := pubsub.RegisterPublisher[string](streamID)
				defer pubsub.UnRegisterPublisher[string](publisher)

				publisher.PublishContent(content1)
				publisher.PublishContent(content2)
				publisher.PublishContent(content3)

				<-done

				pubsub.TryRemoveStreams(streamID)

				Eventually(func() int { return len(eventResult) }).Should(Equal(3))

				Expect(content1).To(Equal(eventResult[0].GetContent()))
				Expect(content2).To(Equal(eventResult[1].GetContent()))
				Expect(content3).To(Equal(eventResult[2].GetContent()))
			})
		})
	})

	Describe("StreamMetrics", func() {
		var (
			streamID pubsub.StreamID
			topic    = "testMetrics"
		)

		BeforeEach(func() {
			streamID, _ = pubsub.AddOrReplaceStream[string](topic)
		})

		AfterEach(func() {
			pubsub.TryRemoveStreams(streamID)
			_, err := pubsub.GetDescription(streamID)
			Expect(err).ToNot(BeNil())
		})

		It("counts in and outgoing events", func() {
			wg := sync.WaitGroup{}
			pub, err := pubsub.RegisterPublisher[string](streamID)
			Expect(err).To(BeNil())
			defer pubsub.UnRegisterPublisher(pub)
			sub, err := pubsub.SubscribeByTopicID[string](streamID, func(event events.Event[string]) {
				wg.Done()
			})
			Expect(err).To(BeNil())
			defer pubsub.Unsubscribe(sub)

			maxRange := uint64(500)

			wg.Add(int(maxRange))

			wg.Go(func() {
				defer GinkgoRecover()
				for range maxRange {
					pub.PublishContent("test")
				}
			})

			wg.Wait()

			metrics, err := pubsub.Metrics(streamID)
			Expect(err).To(BeNil())
			Eventually(metrics.NumEventsIn).Should(Equal(maxRange))
			Eventually(metrics.NumEventsOut).Should(Equal(maxRange))
			Eventually(metrics.NumInEventsEqualsNumOutEvents).Should(BeTrue())

		})
	})
})
