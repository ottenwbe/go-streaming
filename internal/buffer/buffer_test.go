package buffer_test

import (
	"go-stream-processing/internal/buffer"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/selection"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Buffer", func() {

	Describe("LimitedSimpleAsyncBuffer", func() {

		var (
			buf buffer.Buffer[string]
			e1  events.Event[string]
			e2  events.Event[string]
			e3  events.Event[string]
		)

		BeforeEach(func() {
			buf = buffer.NewLimitedSimpleAsyncBuffer[string](1)
			e1 = events.NewEvent("e1")
			e2 = events.NewEvent("e2")
			e3 = events.NewEvent("e3")
		})

		AfterEach(func() {
			buf.StopBlocking()
		})

		Context("Buffer with max length 1", func() {
			It("should throw an error when a second event is added", func() {

				err1 := buf.AddEvent(e1)
				err2 := buf.AddEvent(e2)

				Expect(err1).To(BeNil())
				Expect(err2).To(Not(BeNil()))
			})
			It("should throw an error when more than two events are added", func() {

				err1 := buf.AddEvents([]events.Event[string]{e1, e2, e3})
				Expect(err1).To(Not(BeNil()))
			})
		})

	})

	Describe("ConsumableAsyncBuffer", func() {
		var (
			buf buffer.Buffer[string]
			e1  events.Event[string]
		)

		BeforeEach(func() {
			buf = buffer.NewConsumableAsyncBuffer[string](selection.NewSelectNextPolicy[string]())
			e1 = events.NewEvent("e1")
		})

		AfterEach(func() {
			buf.StopBlocking()
		})

		It("consumes events based on policy", func() {
			buf.AddEvent(e1)
			res := buf.GetAndConsumeNextEvents()
			Expect(res).To(HaveLen(1))
			Expect(res[0]).To(Equal(e1))
		})

		It("blocks until policy is satisfied", func() {
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				buf.GetAndConsumeNextEvents()
				close(done)
			}()
			Consistently(done).ShouldNot(BeClosed())
			buf.AddEvent(e1)
			Eventually(done).Should(BeClosed())
		})

		It("returns empty when stopped while waiting", func() {
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				res := buf.GetAndConsumeNextEvents()
				Expect(res).To(BeEmpty())
				close(done)
			}()
			Consistently(done).ShouldNot(BeClosed())
			buf.StopBlocking()
			Eventually(done).Should(BeClosed())
		})
	})

	Describe("SimpleAsyncBuffer", func() {

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
					defer GinkgoRecover()
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
					defer GinkgoRecover()
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
					defer GinkgoRecover()
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

		Context("Concurrency", func() {
			It("handles concurrent writes correctly", func() {
				const numRoutines = 10
				const numEvents = 100

				start := make(chan struct{})
				done := make(chan struct{})

				for i := 0; i < numRoutines; i++ {
					go func() {
						defer GinkgoRecover()
						<-start
						for j := 0; j < numEvents; j++ {
							buf.AddEvent(events.NewEvent("data"))
						}
						done <- struct{}{}
					}()
				}

				close(start)
				for i := 0; i < numRoutines; i++ {
					<-done
				}

				Expect(buf.Len()).To(Equal(numRoutines * numEvents))
			})
		})

		Context("StopBlocking", func() {
			It("unblocks GetAndConsumeNextEvents returning nil element", func() {
				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					res := buf.GetAndConsumeNextEvents()
					Expect(res).To(HaveLen(1))
					Expect(res[0]).To(BeNil())
					close(done)
				}()

				Consistently(done).ShouldNot(BeClosed())
				buf.StopBlocking()
				Eventually(done).Should(BeClosed())
			})
		})
	})
})
