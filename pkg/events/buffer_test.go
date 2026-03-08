package events_test

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var _ = Describe("Buffer", func() {

	Describe("EventBuffer", func() {
		It("should behave like a standard slice-based buffer", func() {
			buf := events.NewEventBuffer[string]()
			buf.Add(events.NewEvent("1"))
			buf.Add(events.NewEvent("2"))

			Expect(buf.Len()).To(Equal(2))
			Expect(buf.Get(0).GetContent()).To(Equal("1"))

			buf.Remove(1)
			Expect(buf.Len()).To(Equal(1))
			Expect(buf.Get(0).GetContent()).To(Equal("2"))
		})
	})

	Describe("LimitedSimpleAsyncBuffer", func() {

		var (
			buf events.Buffer[string]
			e1  events.Event[string]
			e2  events.Event[string]
			e3  events.Event[string]
		)

		BeforeEach(func() {
			buf = events.NewLimitedSimpleAsyncBuffer[string](1)
			e1 = events.NewEvent("e1")
			e2 = events.NewEvent("e2")
			e3 = events.NewEvent("e3")
		})

		AfterEach(func() {
			buf.StopBlocking()
		})

		Context("Buffer with max length 1", func() {
			It("should block when a second event is added", func() {
				err := buf.AddEvent(context.Background(), e1)
				Expect(err).To(BeNil())

				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					err := buf.AddEvent(context.Background(), e2)
					Expect(err).To(BeNil())
					close(done)
				}()

				Consistently(done).ShouldNot(BeClosed())

				buf.GetAndConsumeNextEvents(context.Background())

				Eventually(done).Should(BeClosed())
				Expect(buf.Len()).To(Equal(1))
			})
			It("should throw an error when more than two events are added", func() {

				err1 := buf.AddEvents(context.Background(), []events.Event[string]{e1, e2, e3})
				Expect(err1).To(HaveOccurred())
				Expect(errors.Is(err1, events.ErrLimitExceeded)).To(BeTrue())
			})
		})

	})

	Describe("LimitedConsumableAsyncBuffer", func() {
		It("should respect the limit and block on AddEvent", func() {
			// Limit 1, SelectNext policy
			buf := events.NewLimitedConsumableAsyncBuffer[string](events.NewSelectNextPolicy[string](), 1)
			defer buf.StopBlocking()

			e1 := events.NewEvent("e1")
			e2 := events.NewEvent("e2")

			err := buf.AddEvent(context.Background(), e1)
			Expect(err).To(BeNil())

			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				err := buf.AddEvent(context.Background(), e2)
				Expect(err).To(BeNil())
				close(done)
			}()

			Consistently(done).ShouldNot(BeClosed())

			buf.GetAndConsumeNextEvents(context.Background())
			Eventually(done).Should(BeClosed())
		})
	})

	Describe("ConsumableAsyncBuffer", func() {
		var (
			buf events.Buffer[string]
			e1  events.Event[string]
		)

		BeforeEach(func() {
			buf = events.NewConsumableAsyncBuffer[string](events.NewSelectNextPolicy[string]())
			e1 = events.NewEvent("e1")
		})

		AfterEach(func() {
			buf.StopBlocking()
		})

		It("consumes events based on policy", func() {
			buf.AddEvent(context.Background(), e1)
			res, _ := buf.GetAndConsumeNextEvents(context.Background())
			Expect(res).To(HaveLen(1))
			Expect(res[0]).To(Equal(e1))
		})

		It("blocks until policy is satisfied", func() {
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				buf.GetAndConsumeNextEvents(context.Background())
				close(done)
			}()
			Consistently(done).ShouldNot(BeClosed())
			buf.AddEvent(context.Background(), e1)
			Eventually(done).Should(BeClosed())
		})

		It("returns empty when stopped while waiting", func() {
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				res, _ := buf.GetAndConsumeNextEvents(context.Background())
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
			buf events.Buffer[string]
			e1  events.Event[string]
			e2  events.Event[string]
			e3  events.Event[string]
		)

		BeforeEach(func() {
			buf = events.NewSimpleAsyncBuffer[string]()
			e1 = events.NewEvent("e1")
			e2 = events.NewEvent("e2")
			e3 = events.NewEvent("e3")
		})

		AfterEach(func() {
			buf.StopBlocking()
		})

		Context("GetAndConsumeNextEvent", func() {
			It("reads and deletes in a fifo manner events from a buffer", func() {

				buf.AddEvent(context.Background(), e1)
				buf.AddEvent(context.Background(), e2)
				resultEvent, _ := buf.GetAndConsumeNextEvents(context.Background())

				Expect(resultEvent[0]).To(Equal(e1))
				Expect(buf.Len()).To(Equal(1))
			})
		})
		Context("PeekNextEvent", func() {
			It("reads events w/o deleting them from a buffer", func() {

				buf.AddEvent(context.Background(), e1)
				r, _ := buf.PeekNextEvent(context.Background())

				Expect(r).To(Equal(e1))
				Expect(buf.Len()).To(Equal(1))
			})
		})
		Context("Dump", func() {
			It("dumps all buffered events", func() {

				buffer := events.NewSimpleAsyncBuffer[string]()
				defer buffer.StopBlocking()

				buffer.AddEvent(context.Background(), e1)
				buffer.AddEvent(context.Background(), e2)

				Expect(buffer.Dump()).To(Equal([]events.Event[string]{e1, e2}))
			})
		})
		Context("AddEvents", func() {
			It("adds all buffered events", func() {

				buf.AddEvent(context.Background(), e1)
				buf.AddEvents(context.Background(), []events.Event[string]{e2, e3})

				Expect(buf.Dump()).To(Equal(events.Arr(e1, e2, e3)))
			})
		})
		Context("AsyncStream PeekNext", func() {
			It("wait for events if not available in buffer", func() {
				buf.AddEvent(context.Background(), e1)
				r1, _ := buf.GetAndConsumeNextEvents(context.Background())
				Expect(r1[0]).To(Equal(e1))

				done := make(chan events.Event[string])
				go func() {
					defer GinkgoRecover()
					e, _ := buf.PeekNextEvent(context.Background())
					done <- e
				}()

				Consistently(done).ShouldNot(Receive())
				buf.AddEvent(context.Background(), e2)
				Eventually(done).Should(Receive(Equal(e2)))
				Expect(buf.Len()).To(Equal(1))
			})
		})
		Context("StopBlocking with PeekNext", func() {
			It("ensures that PeekNextEvent unblocks when stopped", func() {
				done := make(chan events.Event[string])
				go func() {
					defer GinkgoRecover()
					e, _ := buf.PeekNextEvent(context.Background())
					done <- e
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
						e, _ := buf.GetAndConsumeNextEvents(context.Background())
						r = append(r, e...)
					}
					done <- r
				}()

				Consistently(done).ShouldNot(Receive())
				buf.AddEvent(context.Background(), e1)
				buf.AddEvent(context.Background(), e2)
				buf.AddEvent(context.Background(), e3)

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
							buf.AddEvent(context.Background(), events.NewEvent("data"))
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
					res, _ := buf.GetAndConsumeNextEvents(context.Background())
					Expect(res).To(BeNil())
					close(done)
				}()

				Consistently(done).ShouldNot(BeClosed())
				buf.StopBlocking()
				Eventually(done).Should(BeClosed())
			})
		})
	})

	Describe("SortedSimpleAsyncBuffer", func() {
		var (
			buf events.Buffer[string]
			e1  events.Event[string]
			e2  events.Event[string]
			e3  events.Event[string]
		)

		BeforeEach(func() {
			buf = events.NewSortedSimpleAsyncBuffer[string](0)

			// Create events with specific, out-of-order timestamps
			now := time.Now()
			e1 = &events.TemporalEvent[string]{ // The oldest
				Stamp:   events.TimeStamp{StartTime: now},
				Content: "e1",
			}
			e2 = &events.TemporalEvent[string]{ // The newest
				Stamp:   events.TimeStamp{StartTime: now.Add(2 * time.Second)},
				Content: "e2",
			}
			e3 = &events.TemporalEvent[string]{ // In the middle
				Stamp:   events.TimeStamp{StartTime: now.Add(1 * time.Second)},
				Content: "e3",
			}
		})

		AfterEach(func() {
			buf.StopBlocking()
		})

		Context("Adding events", func() {
			It("should sort events by timestamp upon insertion", func() {
				// Add events out of temporal order
				Expect(buf.AddEvent(context.Background(), e2)).To(Succeed())
				Expect(buf.AddEvent(context.Background(), e1)).To(Succeed())
				Expect(buf.AddEvent(context.Background(), e3)).To(Succeed())

				// Dump the buffer and check the order
				dumpedEvents := buf.Dump()
				Expect(dumpedEvents).To(Equal([]events.Event[string]{e1, e3, e2}))
			})

			It("should sort a batch of events by timestamp", func() {
				// Add events out of temporal order in a single batch
				Expect(buf.AddEvents(context.Background(), []events.Event[string]{e2, e1, e3})).To(Succeed())

				// Dump the buffer and check the order
				dumpedEvents := buf.Dump()
				Expect(dumpedEvents).To(Equal([]events.Event[string]{e1, e3, e2}))
			})
		})

		Context("Consuming events", func() {
			It("should consume events in sorted timestamp order", func() {
				Expect(buf.AddEvents(context.Background(), []events.Event[string]{e2, e1, e3})).To(Succeed())
				e1Res, _ := buf.GetAndConsumeNextEvents(context.Background())
				Expect(e1Res).To(Equal([]events.Event[string]{e1}))
				e3Res, _ := buf.GetAndConsumeNextEvents(context.Background())
				Expect(e3Res).To(Equal([]events.Event[string]{e3}))
				e2Res, _ := buf.GetAndConsumeNextEvents(context.Background())
				Expect(e2Res).To(Equal([]events.Event[string]{e2}))
				Expect(buf.Len()).To(Equal(0))
			})
		})

		Context("With Limit", func() {
			It("should block when limit is reached", func() {
				buf = events.NewSortedSimpleAsyncBuffer[string](1)
				defer buf.StopBlocking()

				Expect(buf.AddEvent(context.Background(), e1)).To(Succeed())

				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					Expect(buf.AddEvent(context.Background(), e2)).To(Succeed())
					close(done)
				}()

				Consistently(done).ShouldNot(BeClosed())

				buf.GetAndRemoveNextEvent(context.Background())
				Eventually(done).Should(BeClosed())
			})
		})
	})

	Describe("Iterator", func() {
		It("should iterate over the buffer", func() {
			buf := events.NewSimpleAsyncBuffer[string]()
			buf.AddEvent(context.Background(), events.NewEvent("1"))
			buf.AddEvent(context.Background(), events.NewEvent("2"))

			it := events.NewIterator[string](buf)

			Expect(it.HasNext()).To(BeTrue())
			Expect(it.Next().GetContent()).To(Equal("1"))

			Expect(it.HasNext()).To(BeTrue())
			Expect(it.Next().GetContent()).To(Equal("2"))

			Expect(it.HasNext()).To(BeFalse())
		})
	})
})
