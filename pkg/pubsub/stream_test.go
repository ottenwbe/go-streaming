package pubsub_test

import (
	"fmt"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("localSyncStream", func() {
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
			Expect(err).To(BeNil()) // Stream should still exist
		})
	})

	Context("published events", func() {
		It("should be received", func() {
			var eventResult events.Event[string]
			event := events.NewEvent[string]("test-1")
			done := make(chan bool)

			receiver, _ := pubsub.SubscribeByTopicID[string](streamID)

			go func() {
				eventResult = <-receiver.Notify()
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

var _ = Describe("localAsyncStream", func() {
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
			Expect(err).To(BeNil()) // Stream should still exist
		})

		It("should no longer be subscribable after closing the stream", func() {
			// Close stream (it has no subscribers/publishers yet)
			pubsub.TryRemoveStreams(streamID)

			result, err := pubsub.SubscribeByTopicID[string](streamID)
			Expect(result).To(BeNil())
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
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

			publisher, _ := pubsub.RegisterPublisher[string](streamID)

			publisher.Publish(event1)
			publisher.Publish(event2)
			publisher.Publish(event3)

			go func() {
				fmt.Print("test consumed event")
				eventResult[0] = <-receiver.Notify()
				eventResult[1] = <-receiver.Notify()
				eventResult[2] = <-receiver.Notify()
				fmt.Print("test consumed event finished")
				done <- true
			}()

			<-done

			pubsub.TryRemoveStreams(streamID)

			er1 := eventResult[0].GetContent()
			er2 := eventResult[1].GetContent()

			e1 := event1.GetContent()
			e2 := event2.GetContent()

			fmt.Print(er1)
			fmt.Print(er2)

			Expect(e1).To(Equal(er1))
			Expect(e2).To(Equal(er2))
		})
	})
})
