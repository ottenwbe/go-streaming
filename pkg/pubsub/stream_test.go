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
			desc     pubsub.StreamDescription
		)

		BeforeEach(func() {
			desc = pubsub.MakeStreamDescription[string](topic)
			var err error
			streamID, err = pubsub.AddOrReplaceStreamFromDescription[string](desc)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			pubsub.ForceRemoveStream(streamID)
		})

		Context("description", func() {
			It("should be retrievable", func() {
				retrievedDesc, err := pubsub.GetDescription(streamID)
				Expect(err).To(BeNil())
				Expect(retrievedDesc).To(Equal(desc))
			})
			It("should contain a valid id", func() {
				Expect(streamID.IsNil()).ToNot(BeTrue())
			})
		})

		Context("closing the stream", func() {
			It("without force ensures that the stream receiver is still functioning after trying to close the stream", func() {
				_, err := pubsub.SubscribeByTopicID[string](streamID)
				Expect(err).To(BeNil())

				pubsub.TryRemoveStreams(streamID)

				_, err = pubsub.GetDescription(streamID)
				Expect(err).To(BeNil()) // stream should still exist
			})
		})

		Context("published events", func() {
			It("should be received", func() {
				var eventResult events.Event[string]
				event := events.NewEvent[string]("test-1")
				done := make(chan bool)

				receiver, _ := pubsub.SubscribeByTopicID[string](streamID)

				go func() {
					res, _ := receiver.Next()
					eventResult = res[0]
					done <- true
				}()

				p, _ := pubsub.RegisterPublisher[string](streamID)
				defer pubsub.UnRegisterPublisher[string](p)

				p.Publish(event)
				<-done

				Expect(eventResult.GetContent()).To(Equal(event.GetContent()))
			})
		})
	})

	Describe("localAsyncStream", func() {
		var (
			streamID pubsub.StreamID
			topic    = "test3"
			desc     pubsub.StreamDescription
		)

		BeforeEach(func() {
			desc = pubsub.MakeStreamDescription[string](topic, pubsub.WithAsyncStream(true))
			var err error
			streamID, err = pubsub.AddOrReplaceStreamFromDescription[string](desc)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			pubsub.ForceRemoveStream(streamID)
		})

		Context("description", func() {
			It("should be retrievable", func() {
				retrievedDesc, err := pubsub.GetDescription(streamID)
				Expect(err).To(BeNil())
				Expect(retrievedDesc).To(Equal(desc))
			})
			It("should contain a valid id", func() {
				Expect(streamID.IsNil()).ToNot(BeTrue())
			})
		})

		Context("closing the stream", func() {
			It("without force ensures that the stream receiver is still functioning after trying to close the stream", func() {
				_, err := pubsub.SubscribeByTopicID[string](streamID)
				Expect(err).To(BeNil())

				pubsub.TryRemoveStreams(streamID)

				_, err = pubsub.GetDescription(streamID)
				Expect(err).To(BeNil()) // stream should still exist
			})

			It("should be subscribable (auto-create) after closing the stream", func() {
				// close stream (it has no subscribers/publishers yet)
				pubsub.TryRemoveStreams(streamID)

				result, err := pubsub.SubscribeByTopicID[string](streamID)
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

				s, err := pubsub.SubscribeByTopicID[string](streamID)
				Expect(err).To(BeNil())
				defer pubsub.Unsubscribe[string](s)

				wg.Go(func() {
					for i := range numE {
						e := events.NewEvent[string](fmt.Sprintf("a%v", i))
						p.Publish(e)
					}
				})

				wg.Go(func() {
					for range numE {
						_, _ = s.Next()
					}
				})

				desc2 := pubsub.MakeStreamDescription[string](topic, pubsub.WithAsyncStream(false))
				streamID2, err := pubsub.AddOrReplaceStreamFromDescription[string](desc2)
				Expect(err).To(BeNil())
				defer pubsub.TryRemoveStreams(streamID2)

				wg.Wait()
			})
		})

		Context("publishing and receiving events", func() {
			It("should not block", func() {

				eventResult := make([]events.Event[string], 3)
				event1 := events.NewEvent("test-3-1")
				event2 := events.NewEvent("test-3-2")
				event3 := events.NewEvent("test-3-3")
				done := make(chan bool)

				receiver, _ := pubsub.SubscribeByTopicID[string](streamID)
				defer pubsub.Unsubscribe[string](receiver)

				publisher, _ := pubsub.RegisterPublisher[string](streamID)
				defer pubsub.UnRegisterPublisher[string](publisher)

				publisher.Publish(event1)
				publisher.Publish(event2)
				publisher.Publish(event3)

				go func() {
					r1, _ := receiver.Next()
					eventResult[0] = r1[0]
					r2, _ := receiver.Next()
					eventResult[1] = r2[0]
					r3, _ := receiver.Next()
					eventResult[2] = r3[0]
					done <- true
				}()

				<-done

				pubsub.TryRemoveStreams(streamID)

				er1 := eventResult[0].GetContent()
				er2 := eventResult[1].GetContent()

				e1 := event1.GetContent()
				e2 := event2.GetContent()

				Expect(e1).To(Equal(er1))
				Expect(e2).To(Equal(er2))
			})
		})
	})

	Describe("StreamMetrics", func() {
		var (
			streamID pubsub.StreamID
			topic    = "testMetrics"
			desc     pubsub.StreamDescription
		)

		BeforeEach(func() {
			desc = pubsub.MakeStreamDescription[string](topic)
			streamID, _ = pubsub.AddOrReplaceStreamFromDescription[string](desc)
		})

		AfterEach(func() {
			pubsub.TryRemoveStreams(streamID)
			_, err := pubsub.GetDescription(streamID)
			Expect(err).ToNot(BeNil())
		})

		It("counts in and outgoing events", func() {

			pub, err := pubsub.RegisterPublisher[string](streamID)
			Expect(err).To(BeNil())
			defer pubsub.UnRegisterPublisher(pub)
			sub, err := pubsub.SubscribeByTopicID[string](streamID)
			Expect(err).To(BeNil())
			defer pubsub.Unsubscribe(sub)

			maxRange := uint64(500)

			wg := sync.WaitGroup{}

			wg.Go(func() {
				defer GinkgoRecover()
				for range maxRange {
					sub.Next()
				}
			})

			wg.Go(func() {
				defer GinkgoRecover()
				for range maxRange {
					pub.Publish(events.NewEvent("test"))
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
