package streams_test

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/events"
	"go-stream-processing/streams"
)

var _ = Describe("Stream", func() {
	var stream *streams.LocalSyncStream
	var asyncStream *streams.LocalAsyncStream

	BeforeEach(func() {
		stream = streams.NewLocalSyncStream(streams.NewStreamDescription("test", uuid.New(), false))
		stream.Start()
		asyncStream = streams.NewLocalAsyncStream(streams.NewStreamDescription("test3", uuid.New(), true))
		asyncStream.Start()
	})

	Describe("LocalSyncStream One Event", func() {
		Context("publish and receive one event", func() {
			It("should be consumed", func() {
				var eventResult events.Event
				event := events.NewEvent("k", "test-1")
				bChan := make(chan bool)

				receiver := stream.Subscribe()

				go func() {
					eventResult = <-receiver.Notify
					bChan <- true
				}()

				stream.Publish(event)
				<-bChan

				e1 := eventResult.GetContent("k")
				e2 := event.GetContent("k")
				fmt.Print(e1)

				Expect(e2).To(Equal(e1))
			})
		})
		Describe("LocalAsyncStream ", func() {
			Context("publish and receive one event", func() {
				It("should not block", func() {
					var eventResult []events.Event = make([]events.Event, 3)
					event1 := events.NewEvent("k", "test-3-1")
					event2 := events.NewEvent("k", "test-3-2")
					event3 := events.NewEvent("k", "test-3-3")
					bChan := make(chan bool)

					receiver := asyncStream.Subscribe()

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
					asyncStream.Stop()

					er1 := eventResult[0].GetContent("k")
					er2 := eventResult[1].GetContent("k")

					e1 := event1.GetContent("k")
					e2 := event2.GetContent("k")

					fmt.Print(er1)
					fmt.Print(er2)

					Expect(e1).To(Equal(er1))
					Expect(e2).To(Equal(er2))
				})
			})
		})
	})
})
