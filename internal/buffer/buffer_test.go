package buffer_test

import (
	"sync"

	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/selection"

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
			It("should block when a second event is added", func() {
				err := buf.AddEvent(e1)
				Expect(err).To(BeNil())

				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					err := buf.AddEvent(e2)
					Expect(err).To(BeNil())
					close(done)
				}()

				Consistently(done).ShouldNot(BeClosed())

				buf.GetAndConsumeNextEvents()

				Eventually(done).Should(BeClosed())
				Expect(buf.Len()).To(Equal(1))
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
		Context("AsyncStream PeekNext", func() {
			It("wait for events if not available in buffer", func() {
				buf.AddEvent(e1)
				r1 := buf.GetAndConsumeNextEvents()
				Expect(r1[0]).To(Equal(e1))

				done := make(chan events.Event[string])
				go func() {
					defer GinkgoRecover()
					done <- buf.PeekNextEvent()
				}()

				Consistently(done).ShouldNot(Receive())
				buf.AddEvent(e2)
				Eventually(done).Should(Receive(Equal(e2)))
				Expect(buf.Len()).To(Equal(1))
			})
		})
		Context("StopBlocking with PeekNext", func() {
			It("ensures that PeekNextEvent unblocks when stopped", func() {
				done := make(chan events.Event[string])
				go func() {
					defer GinkgoRecover()
					done <- buf.PeekNextEvent()
				}()

				Consistently(done).ShouldNot(Receive())
				buf.StopBlocking()
				Eventually(done).Should(Receive(BeNil()))
			})
		})
		Context("GetAndRemove", func() {
			It("can be executed multiple times in a row in succession", func() {
				done := make(chan []events.Event[string])
				go func() {
					defer GinkgoRecover()
					var r []events.Event[string]
					for i := 0; i < 3; i++ {
						r = append(r, buf.GetAndConsumeNextEvents()...)
					}
					done <- r
				}()

				Consistently(done).ShouldNot(Receive())
				buf.AddEvent(e1)
				buf.AddEvent(e2)
				buf.AddEvent(e3)

				Eventually(done).Should(Receive(Equal([]events.Event[string]{e1, e2, e3})))
				Expect(buf.Len()).To(Equal(0))
			})
		})

		Context("Concurrency", func() {
			It("handles concurrent writes correctly", func() {
				const numRoutines = 10
				const numEvents = 100

				var wg sync.WaitGroup
				start := make(chan struct{})

				for i := 0; i < numRoutines; i++ {
					wg.Go(func() {
						defer GinkgoRecover()
						<-start
						for j := 0; j < numEvents; j++ {
							buf.AddEvent(events.NewEvent("data"))
						}
					})
				}

				close(start)
				wg.Wait()
				Expect(buf.Len()).To(Equal(numRoutines * numEvents))
			})
		})

		Context("StopBlocking", func() {
			It("unblocks GetAndConsumeNextEvents returning nil element", func() {
				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					res := buf.GetAndConsumeNextEvents()
					Expect(res).To(BeNil())
					close(done)
				}()

				Consistently(done).ShouldNot(BeClosed())
				buf.StopBlocking()
				Eventually(done).Should(BeClosed())
			})
		})
	})
})
