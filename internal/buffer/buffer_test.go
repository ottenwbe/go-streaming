package buffer_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/internal/buffer"
	"go-stream-processing/pkg/events"
)

var _ = Describe("Buffer", func() {

	var (
		buf buffer.Buffer[string]
		e1  events.Event[string]
		e2  events.Event[string]
		e3  events.Event[string]
	)

	BeforeEach(func() {
		buf = buffer.NewSimpleAsyncBuffer[string]()
		e1 = events.NewEvent("e1")
		e2 = events.NewEvent("e2")
		e3 = events.NewEvent("e3")
	})

	AfterEach(func() {
		buf.StopBlocking()
	})

	Describe("SimpleAsyncBuffer", func() {
		Context("GetAndConsumeNextEvent", func() {
			It("reads and deletes in a fifo manner events from a buffer", func() {

				buf.AddEvent(e1)
				buf.AddEvent(e2)
				resultEvent := buf.GetAndConsumeNextEvents()

				Expect(resultEvent[0]).To(Equal(e1))
				Expect(buf.Len()).To(Equal(1))
			})
		})
		Context("PeekNextEvent", func() {
			It("reads events w/o deleting them from a buffer", func() {

				buf.AddEvent(e1)
				r := buf.PeekNextEvent()

				Expect(r).To(Equal(e1))
				Expect(buf.Len()).To(Equal(1))
			})
		})
		Context("Dump", func() {
			It("dumps all buffered events", func() {

				buffer := buffer.NewSimpleAsyncBuffer[string]()
				defer buffer.StopBlocking()

				buffer.AddEvent(e1)
				buffer.AddEvent(e2)

				Expect(buffer.Dump()).To(Equal([]events.Event[string]{e1, e2}))
			})
		})
		Context("AddEvents", func() {
			It("adds all buffered events", func() {

				buf.AddEvent(e1)
				buf.AddEvents([]events.Event[string]{e2, e3})

				Expect(buf.Dump()).To(Equal(events.Arr(e1, e2, e3)))
			})
		})
		Context("Async PeekNext", func() {
			It("wait for events if not available in buffer", func() {

				bChan := make(chan bool)

				buf.AddEvent(e1)
				r1 := buf.GetAndConsumeNextEvents()

				var r2 events.Event[string]
				go func() {
					r2 = buf.PeekNextEvent()
					bChan <- true
				}()
				buf.AddEvent(e2)
				<-bChan

				Expect(r1[0]).To(Equal(e1))
				Expect(r2).To(Equal(e2))
				Expect(buf.Len()).To(Equal(1))
			})
		})
		Context("Flush", func() {
			It("ensures that PeekNextEvent buffers does not get stuck", func() {

				var testing = true

				go func() {
					for testing == true {
						buf.StopBlocking()
					}
				}()

				rEvent := buf.PeekNextEvent()
				testing = false

				Expect(rEvent).To(BeNil())
			})
		})
		Context("GetAndRemove", func() {
			It("can be executed multiple times in a row in succession", func() {

				bChan := make(chan bool)
				r := make([]events.Event[string], 0)

				go func() {
					for i := 0; i < 3; i++ {
						r = append(r, buf.GetAndConsumeNextEvents()...)
					}
					bChan <- true
				}()
				buf.AddEvent(e1)
				buf.AddEvent(e2)
				buf.AddEvent(e3)
				<-bChan

				Expect(r[0]).To(Equal(e1))
				Expect(r[1]).To(Equal(e2))
				Expect(buf.Len()).To(Equal(0))
			})
		})
	})
})
