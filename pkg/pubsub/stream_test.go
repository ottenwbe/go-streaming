package pubsub_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/pkg/events"
	pubsub2 "go-stream-processing/pkg/pubsub"
)

var _ = Describe("Stream", func() {
	var stream pubsub2.Stream[string]
	var asyncStream pubsub2.Stream[string]

	BeforeEach(func() {
		stream, _ = pubsub2.GetOrCreateStream[string]("test", false)
		stream.Run()
		asyncStream, _ = pubsub2.GetOrCreateStream[string]("test3", true)
		asyncStream.Run()
	})

	Describe("LocalSyncStream One Event", func() {
		Context("publish and receive one event", func() {
			It("should be consumed", func() {
				var eventResult events.Event[string]
				event := events.NewEvent("test-1")
				bChan := make(chan bool)

				receiver, _ := pubsub2.Subscribe[string](stream.ID())

				go func() {
					eventResult = <-receiver.Notify
					bChan <- true
				}()

				stream.Publish(event)
				<-bChan

				e1 := eventResult.GetContent()
				e2 := event.GetContent()
				fmt.Print(e1)

				Expect(e2).To(Equal(e1))
			})
		})
		Describe("LocalAsyncStream ", func() {
			Context("publish and receive one event", func() {
				It("should not block", func() {
					var eventResult []events.Event[string] = make([]events.Event[string], 3)
					event1 := events.NewEvent("test-3-1")
					event2 := events.NewEvent("test-3-2")
					event3 := events.NewEvent("test-3-3")
					bChan := make(chan bool)

					receiver, _ := pubsub2.Subscribe[string](asyncStream.ID())

					asyncStream.Publish(event1)
					asyncStream.Publish(event2)
					asyncStream.Publish(event3)

					go func() {
						fmt.Print("test consumed event")
						eventResult[0] = <-receiver.Notify
						eventResult[1] = <-receiver.Notify
						eventResult[2] = <-receiver.Notify
						fmt.Print("test consumed event finished")
						bChan <- true
					}()

					<-bChan
					asyncStream.TryClose()

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
			It("should be closable", func() {

				asyncStream := pubsub2.NewLocalAsyncStream[string](pubsub2.MakeStreamDescription("close", true))
				asyncStream.Run()

				asyncStream.TryClose()

				result, _ := pubsub2.Subscribe[string](asyncStream.ID())

				Expect(result).To(BeNil())
			})

		})
	})
})
