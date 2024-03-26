package buffer_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
)

var _ = Describe("Buffer", func() {

	Describe("Async Buffer", func() {
		Context("GetAndRemoveNextEvent", func() {
			It("reads and deletes in a fifo manner events from a buffer", func() {
				buffer := buffer.NewAsyncBuffer()
				defer buffer.Flush()

				e1, _ := events.NewEvent("e1")
				e2, _ := events.NewEvent("e2")

				buffer.AddEvent(e1)
				buffer.AddEvent(e2)
				resultEvent := buffer.GetAndRemoveNextEvent()

				Expect(resultEvent).To(Equal(e1))
				Expect(buffer.Len()).To(Equal(1))
			})
		})
		Context("GetNextEvent", func() {
			It("reads events w/o deleting them from a buffer", func() {
				buffer := buffer.NewAsyncBuffer()
				defer buffer.Flush()

				e, _ := events.NewEvent("e1")

				buffer.AddEvent(e)
				r := buffer.GetNextEvent()

				Expect(r).To(Equal(e))
				Expect(buffer.Len()).To(Equal(1))
			})
		})
		Context("Dump", func() {
			It("dumps all buffered events", func() {
				e1, _ := events.NewEvent("e1")
				e2, _ := events.NewEvent("e2")

				buffer := buffer.NewAsyncBuffer()
				defer buffer.Flush()

				buffer.AddEvent(e1)
				buffer.AddEvent(e2)

				Expect(buffer.Dump()).To(Equal([]events.Event{e1, e2}))
			})
		})
		Context("AddEvents", func() {
			It("adds all buffered events", func() {
				e1, _ := events.NewEvent("e1")
				e2, _ := events.NewEvent("e2")
				e3, _ := events.NewEvent("e3")

				buffer := buffer.NewAsyncBuffer()
				defer buffer.Flush()

				buffer.AddEvent(e1)
				buffer.AddEvents([]events.Event{e2, e3})

				Expect(buffer.Dump()).To(Equal([]events.Event{e1, e2, e3}))
			})
		})
		Context("Remove events", func() {
			It("reduces the size of the buffer", func() {
				buffer := buffer.NewAsyncBuffer()
				defer buffer.Flush()

				e1, _ := events.NewEvent("e1")
				buffer.AddEvent(e1)
				buffer.RemoveNextEvent()

				Expect(buffer.Len()).To(Equal(0))
			})
			It("does not run into an error when no event is present", func() {
				buffer := buffer.NewAsyncBuffer()
				defer buffer.Flush()

				buffer.RemoveNextEvent()

				Expect(buffer.Len()).To(Equal(0))
			})
		})
		Context("GetNextEvent function", func() {
			It("wait for events if not available in buffer", func() {
				buffer := buffer.NewAsyncBuffer()
				defer buffer.Flush()

				e1, _ := events.NewEvent("e1")
				e2, _ := events.NewEvent("e2")
				bChan := make(chan bool)

				buffer.AddEvent(e1)
				r1 := buffer.GetAndRemoveNextEvent()

				r2 := r1
				go func() {
					r2 = buffer.GetNextEvent()
					bChan <- true
				}()
				buffer.AddEvent(e2)
				<-bChan

				Expect(r1).To(Equal(e1))
				Expect(r2).To(Equal(e2))
				Expect(buffer.Len()).To(Equal(1))
			})
		})
		Context("flush", func() {
			It("ensures that GetNextEvent buffers does not get stuck", func() {
				buffer := buffer.NewAsyncBuffer()
				var rEvent events.Event
				var testing = true

				go func() {
					for testing == true {
						buffer.Flush()
					}
				}()

				rEvent = buffer.GetNextEvent()
				testing = false

				Expect(rEvent).To(BeNil())
			})
		})
		Context("GetAndRemove", func() {
			It("can be executed multiple times in a row in succession", func() {
				buffer := buffer.NewAsyncBuffer()
				defer buffer.Flush()

				e1, _ := events.NewEvent("e1")
				e2, _ := events.NewEvent("e2")
				e3, _ := events.NewEvent("e3")
				bChan := make(chan bool)
				r := make([]events.Event, 0)

				go func() {
					for i := 0; i < 3; i++ {
						r = append(r, buffer.GetAndRemoveNextEvent())
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
