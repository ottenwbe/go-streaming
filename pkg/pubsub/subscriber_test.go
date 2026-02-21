package pubsub

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

var handler = func(event events.Event[string]) {}

var _ = Describe("Subscriber", func() {

	Describe("SubscriberID", func() {
		It("should return its string representation", func() {
			uid := uuid.New()
			id := SubscriberID(uid)
			Expect(id.String()).To(Equal(uid.String()))
		})
	})

	Describe("defaultSubscriber (Unbuffered)", func() {
		var (
			rec      *defaultSubscriber[string]
			streamID StreamID
			result   events.Event[string]
		)

		BeforeEach(func() {
			streamID = MakeStreamID[string]("test-topic")

			rec = &defaultSubscriber[string]{
				streamID: streamID,
				iD:       SubscriberID(uuid.New()),
				notify: func(event events.Event[string]) {
					result = event
				},
			}
			rec.active.Store(true)
		})

		It("should return the correct StreamID", func() {
			Expect(rec.StreamID()).To(Equal(streamID))
		})

		It("should return the correct ID", func() {
			Expect(rec.ID()).NotTo(BeNil())
		})

		It("should consume events", func() {
			event := events.NewEvent("test-event")
			go func() {
				rec.doNotify(event)
			}()

			Eventually(
				func() events.Event[string] {
					return result
				}).Should(Equal(event))
		})

		It("should stop notifying after close", func() {
			rec.close()
			result = nil
			rec.doNotify(events.NewEvent("test-event"))
			Expect(result).To(BeNil())
		})
	})

	Describe("bufferedSubscriber", func() {
		var (
			rec      Subscriber[string]
			streamID StreamID
			nMap     *notificationMap[string]
			result   events.Event[string]
		)

		BeforeEach(func() {
			streamID = MakeStreamID[string]("test-topic-buffered")
			nMap = newNotificationMap[string](MakeSubscriberDescription(), newStreamMetrics())
			var err error
			rec, err = nMap.newSubscriber(streamID, func(event events.Event[string]) {
				result = event
			})
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			_ = nMap.close()
		})

		It("should return the correct StreamID", func() {
			Expect(rec.StreamID()).To(Equal(streamID))
		})

		It("should return the correct ID", func() {
			Expect(rec.ID()).NotTo(BeNil())
		})

		It("should consume events asynchronously", func() {
			event := events.NewEvent("test-buffered")
			rec.doNotify(event)

			Eventually(func() events.Event[string] { return result }).To(Equal(event))
		})
	})

	Describe("bufferedSubscriber with limit", func() {
		var (
			rec      Subscriber[string]
			streamID StreamID
			nMap     *notificationMap[string]
			result   events.Event[string]
			start    atomic.Bool
		)

		BeforeEach(func() {
			streamID = MakeStreamID[string]("test-topic-buffered-limit")
			nMap = newNotificationMap[string](MakeSubscriberDescription(SubscriberWithBufferCapacity(1)), newStreamMetrics())
			var err error
			rec, err = nMap.newSubscriber(streamID, func(event events.Event[string]) {
				for !start.Load() {
				}
				result = event
			})
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			_ = nMap.close()
		})

		It("should apply backpressure when limit is reached", func() {
			event1 := events.NewEvent("e1")
			event2 := events.NewEvent("e2")
			event3 := events.NewEvent("e3")

			start.Store(false)

			rec.doNotify(event1) // Moves to subscriber function
			rec.doNotify(event2) // Fills buffer (cap 1)

			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				rec.doNotify(event3) // Should block
				close(done)
			}()

			Consistently(done).ShouldNot(BeClosed())

			start.Store(true)

			Eventually(func() events.Event[string] {
				return result
			}).To(Equal(event3))

			Eventually(done).Should(BeClosed())
		})
	})

	Describe("notificationMap", func() {
		var (
			nMap *notificationMap[string]
			sID  StreamID
			wg   sync.WaitGroup
		)

		BeforeEach(func() {
			nMap = newNotificationMap[string](MakeSubscriberDescription(), newStreamMetrics())
			sID = MakeStreamID[string]("topic")
			nMap.start()
		})

		AfterEach(func() {
			_ = nMap.close()
		})

		It("should create new receivers", func() {
			rec, err := nMap.newSubscriber(sID, handler)
			Expect(err).To(BeNil())
			Expect(rec).NotTo(BeNil())
			Expect(nMap.len()).To(Equal(1))
		})

		It("should notify all receivers", func() {
			var (
				e1, e2 events.Event[string]
			)

			rec1, _ := nMap.newSubscriber(sID, func(event events.Event[string]) {
				e1 = event
				wg.Done()
			})
			rec2, _ := nMap.newSubscriber(sID, func(event events.Event[string]) {
				e2 = event
				wg.Done()
			})
			defer Unsubscribe[string](rec1)
			defer Unsubscribe[string](rec2)

			wg.Add(2)

			event := events.NewEvent("broadcast")
			wg.Go(func() { _ = nMap.notify(event) })

			wg.Wait()

			Expect(e1).To(Equal(event))
			Expect(e2).To(Equal(event))
		})

		It("should remove receivers", func() {
			rec, err := nMap.newSubscriber(sID, handler)
			Expect(err).To(BeNil())
			Expect(nMap.len()).To(Equal(1))
			nMap.remove(rec.ID())
			Expect(nMap.len()).To(Equal(0))
		})

		It("should clear all receivers", func() {
			nMap.newSubscriber(sID, handler)
			nMap.newSubscriber(sID, handler)
			Expect(nMap.len()).To(Equal(2))
			_ = nMap.close()
			Expect(nMap.len()).To(Equal(0))
		})

		It("should return error when policy is set for single subscriber", func() {
			_, err := nMap.newSubscriber(sID, handler, SubscriberWithSelectionPolicy(selection.MakePolicy(selection.CountingWindow, 0, 0, time.Now(), time.Nanosecond, time.Nanosecond)))
			Expect(err).To(Equal(SubscriberPolicyError))
		})
	})
})
