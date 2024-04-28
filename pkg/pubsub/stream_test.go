package pubsub_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
)

var _ = Describe("localSyncStream", func() {
	var (
		stream pubsub.Stream[string]
		topic  = "test"
	)

	BeforeEach(func() {
		stream = pubsub.NewStreamD[string](pubsub.MakeStreamDescription[string](topic, false))
		stream.Run()
		pubsub.AddOrReplaceStream(stream)
	})

	AfterEach(func() {
		pubsub.ForceRemoveStream(stream.ID())
	})

	Context("description", func() {
		It("should be retrievable", func() {
			Expect(stream.Description()).To(Equal(pubsub.MakeStreamDescription[string](topic, false)))
		})
		It("should contain a valid id", func() {
			Expect(stream.ID().IsNil()).ToNot(BeTrue())
		})
	})

	Context("closing the stream", func() {
		It("forcefully ensures that all resources are cleaned up", func() {
			pubsub.Subscribe[string](stream.ID())
			Expect(stream.HasPublishersOrSubscribers()).To(BeTrue())
			stream.ForceClose()
			Expect(stream.HasPublishersOrSubscribers()).To(BeFalse())
		})
		It("without force ensures that the stream receiver is still functioning after trying to close the stream", func() {
			pubsub.Subscribe[string](stream.ID())
			Expect(stream.HasPublishersOrSubscribers()).To(BeTrue())
			stream.TryClose()
			Expect(stream.HasPublishersOrSubscribers()).To(BeTrue())
		})
	})

	Context("published events", func() {
		It("should be received", func() {
			var eventResult events.Event[string]
			event := events.NewEvent("test-1")
			bChan := make(chan bool)

			receiver, _ := pubsub.Subscribe[string](stream.ID())

			go func() {
				eventResult = <-receiver.Notify()
				bChan <- true
			}()

			p, _ := pubsub.RegisterPublisher[string](stream.ID())
			defer pubsub.UnRegisterPublisher[string](p)

			p.Publish(event)
			<-bChan

			Expect(eventResult.GetContent()).To(Equal(event.GetContent()))
		})
	})
})

var _ = Describe("localAsyncStream", func() {
	var (
		stream pubsub.Stream[string]
		topic  = "test3"
	)

	BeforeEach(func() {
		stream = pubsub.NewStreamD[string](pubsub.MakeStreamDescription[string](topic, true))
		stream.Run()
		pubsub.AddOrReplaceStream(stream)
	})

	AfterEach(func() {
		pubsub.ForceRemoveStream(stream.ID())
	})

	Context("description", func() {
		It("should be retrievable", func() {
			Expect(stream.Description()).To(Equal(pubsub.MakeStreamDescription[string](topic, true)))
		})
		It("should contain a valid id", func() {
			Expect(stream.ID().IsNil()).ToNot(BeTrue())
		})
	})

	Context("closing the stream", func() {
		It("forcefully ensures that all resources are cleaned up", func() {
			pubsub.Subscribe[string](stream.ID())
			Expect(stream.HasPublishersOrSubscribers()).To(BeTrue())
			stream.ForceClose()
			Expect(stream.HasPublishersOrSubscribers()).To(BeFalse())
		})
		It("without force ensures that the stream receiver is still functioning after trying to close the stream", func() {
			pubsub.Subscribe[string](stream.ID())
			Expect(stream.HasPublishersOrSubscribers()).To(BeTrue())
			stream.TryClose()
			Expect(stream.HasPublishersOrSubscribers()).To(BeTrue())
		})
		It("should no longer be subscribable after closing the stream", func() {
			stream.TryClose()
			result, err := pubsub.Subscribe[string](stream.ID())
			Expect(result).To(BeNil())
			Expect(err).ToNot(BeNil())
		})
	})

	Context("publishing and receiving events", func() {
		It("should not block", func() {
			var eventResult []events.Event[string] = make([]events.Event[string], 3)
			event1 := events.NewEvent("test-3-1")
			event2 := events.NewEvent("test-3-2")
			event3 := events.NewEvent("test-3-3")
			bChan := make(chan bool)

			receiver, _ := pubsub.Subscribe[string](stream.ID())

			publisher, _ := pubsub.RegisterPublisher[string](stream.ID())

			publisher.Publish(event1)
			publisher.Publish(event2)
			publisher.Publish(event3)

			go func() {
				fmt.Print("test consumed event")
				eventResult[0] = <-receiver.Notify()
				eventResult[1] = <-receiver.Notify()
				eventResult[2] = <-receiver.Notify()
				fmt.Print("test consumed event finished")
				bChan <- true
			}()

			<-bChan
			stream.TryClose()

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
