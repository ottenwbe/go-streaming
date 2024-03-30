package buffer_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
)

var _ = Describe("Buffer", func() {

	var (
		buf buffer.Buffer
	)

	BeforeEach(func() {
		buf = buffer.NewSimpleAsyncBuffer()
	})

	AfterEach(func() {
		buf.StopBlocking()
	})

	Describe("SimpleAsyncBuffer", func() {
		Context("GetAndConsumeNextEvent", func() {
			It("reads and deletes in a fifo manner events from a buffer", func() {
				buffer := buffer.NewSimpleAsyncBuffer()
				defer buffer.StopBlocking()

				e1 := events.NewEvent("key", "e1")
				e2 := events.NewEvent("key", "e2")

				buffer.AddEvent(e1)
				buffer.AddEvent(e2)
				resultEvent := buffer.GetAndConsumeNextEvents()

				Expect(resultEvent[0]).To(Equal(e1))
				Expect(buffer.Len()).To(Equal(1))
			})
		})
		Context("PeekNextEvent", func() {
			It("reads events w/o deleting them from a buffer", func() {
				buffer := buffer.NewSimpleAsyncBuffer()
				defer buffer.StopBlocking()

				e := events.NewEvent("key", "e1")

				buffer.AddEvent(e)
				r := buffer.PeekNextEvent()

				Expect(r).To(Equal(e))
				Expect(buffer.Len()).To(Equal(1))
			})
		})
		Context("Dump", func() {
			It("dumps all buffered events", func() {
				e1 := events.NewEvent("key", "e1")
				e2 := events.NewEvent("key", "e2")

				buffer := buffer.NewSimpleAsyncBuffer()
				defer buffer.StopBlocking()

				buffer.AddEvent(e1)
				buffer.AddEvent(e2)

				Expect(buffer.Dump()).To(Equal([]events.Event{e1, e2}))
			})
		})
		Context("AddEvents", func() {
			It("adds all buffered events", func() {
				e1 := events.NewEvent("key", "e1")
				e2 := events.NewEvent("key", "e2")
				e3 := events.NewEvent("key", "e3")

				buffer := buffer.NewSimpleAsyncBuffer()
				defer buffer.StopBlocking()

				buffer.AddEvent(e1)
				buffer.AddEvents([]events.Event{e2, e3})

				Expect(buffer.Dump()).To(Equal([]events.Event{e1, e2, e3}))
			})
		})
		Context("Async PeekNext", func() {
			It("wait for events if not available in buffer", func() {
				buffer := buffer.NewSimpleAsyncBuffer()
				defer buffer.StopBlocking()

				e1 := events.NewEvent("key", "e1")
				e2 := events.NewEvent("key", "e2")
				bChan := make(chan bool)

				buffer.AddEvent(e1)
				r1 := buffer.GetAndConsumeNextEvents()

				var r2 events.Event
				go func() {
					r2 = buffer.PeekNextEvent()
					bChan <- true
				}()
				buffer.AddEvent(e2)
				<-bChan

				Expect(r1[0]).To(Equal(e1))
				Expect(r2).To(Equal(e2))
				Expect(buffer.Len()).To(Equal(1))
			})
		})
		Context("Flush", func() {
			It("ensures that PeekNextEvent buffers does not get stuck", func() {
				buffer := buffer.NewSimpleAsyncBuffer()
				var rEvent events.Event
				var testing = true

				go func() {
					for testing == true {
						buffer.StopBlocking()
					}
				}()

				rEvent = buffer.PeekNextEvent()
				testing = false

				Expect(rEvent).To(BeNil())
			})
		})
		Context("GetAndRemove", func() {
			It("can be executed multiple times in a row in succession", func() {
				buffer := buffer.NewSimpleAsyncBuffer()
				defer buffer.StopBlocking()

				e1 := events.NewEvent("key", "e1")
				e2 := events.NewEvent("key", "e2")
				e3 := events.NewEvent("key", "e3")
				bChan := make(chan bool)
				r := make([]events.Event, 0)

				go func() {
					for i := 0; i < 3; i++ {
						r = append(r, buffer.GetAndConsumeNextEvents()...)
					}
					bChan <- true
				}()
				buffer.AddEvent(e1)
				buffer.AddEvent(e2)
				buffer.AddEvent(e3)
				<-bChan

				Expect(r[0]).To(Equal(e1))
				Expect(r[1]).To(Equal(e2))
				Expect(buffer.Len()).To(Equal(0))
			})
		})
	})
})
