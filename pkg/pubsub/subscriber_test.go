package pubsub

import (
	"sync"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

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
		)

		BeforeEach(func() {
			streamID = MakeStreamID[string]("test-topic")
			rec = &defaultSubscriber[string]{
				streamID: streamID,
				iD:       SubscriberID(uuid.New()),
				notify:   make(chan events.Event[string]),
			}
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

			received, more := rec.Next()
			Expect(more).To(BeTrue())
			Expect(received).To(Equal(event))
		})

		It("should close the channel", func() {
			rec.close()
			Eventually(rec.notify).Should(BeClosed())
		})
	})

	Describe("bufferedSubscriber", func() {
		var (
			rec      Subscriber[string]
			streamID StreamID
			nMap     *notificationMap[string]
			ch       events.EventChannel[string]
		)

		BeforeEach(func() {
			streamID = MakeStreamID[string]("test-topic-buffered")
			ch = make(events.EventChannel[string])
			nMap = newNotificationMap[string](MakeSubscriberDescription(SubscriberIsAsync(true)), ch, newStreamMetrics())
			var err error
			rec, err = nMap.newSubscriber(streamID)
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

			received, more := rec.Next()
			Expect(more).To(BeTrue())
			Expect(received).To(Equal(event))
		})

		It("should close resources", func() {
			rec.close()
			Eventually(func() bool {
				_, more := rec.Next()
				return more
			}).Should(BeFalse())
		})
	})

	Describe("bufferedSubscriber with limit", func() {
		var (
			rec      Subscriber[string]
			streamID StreamID
			nMap     *notificationMap[string]
			ch       events.EventChannel[string]
		)

		BeforeEach(func() {
			streamID = MakeStreamID[string]("test-topic-buffered-limit")
			ch = make(events.EventChannel[string])
			nMap = newNotificationMap[string](MakeSubscriberDescription(SubscriberIsAsync(true), SubscriberWithBufferCapacity(1)), ch, newStreamMetrics())
			var err error
			rec, err = nMap.newSubscriber(streamID)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			_ = nMap.close()
		})

		It("should apply backpressure when limit is reached", func() {
			event1 := events.NewEvent("e1")
			event3 := events.NewEvent("e3")

			rec.doNotify(event1) // Moves to notify channel (blocked there)

			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				rec.doNotify(event3) // Should block
				close(done)
			}()

			Consistently(done).ShouldNot(BeClosed())

			v, more := rec.Next()
			Expect(more).To(BeTrue())
			Expect(v).To(Equal(event1))

			Eventually(done).Should(BeClosed())
		})
	})

	Describe("notificationMap", func() {
		var (
			nMap *notificationMap[string]
			sID  StreamID
			ch   events.EventChannel[string]
		)

		BeforeEach(func() {
			ch = make(events.EventChannel[string])
			nMap = newNotificationMap[string](MakeSubscriberDescription(), ch, newStreamMetrics())
			sID = MakeStreamID[string]("topic")
			nMap.start()
		})

		AfterEach(func() {
			_ = nMap.close()
		})

		It("should create new receivers", func() {
			rec, err := nMap.newSubscriber(sID)
			Expect(err).To(BeNil())
			Expect(rec).NotTo(BeNil())
			Expect(nMap.len()).To(Equal(1))
		})

		It("should notify all receivers", func() {
			rec1, _ := nMap.newSubscriber(sID)
			rec2, _ := nMap.newSubscriber(sID)

			event := events.NewEvent("broadcast")
			go func() { ch <- event }()

			var (
				e1, e2       events.Event[string]
				more1, more2 bool
				wg           sync.WaitGroup
			)

			wg.Go(func() {
				e1, more1 = rec1.Next()
			})
			wg.Go(func() {
				e2, more2 = rec2.Next()
			})
			wg.Wait()

			Expect(more1).To(BeTrue())
			Expect(more2).To(BeTrue())
			Expect(e1).To(Equal(event))
			Expect(e2).To(Equal(event))
		})

		It("should remove receivers", func() {
			rec, err := nMap.newSubscriber(sID)
			Expect(err).To(BeNil())
			Expect(nMap.len()).To(Equal(1))
			nMap.remove(rec.ID())
			Expect(nMap.len()).To(Equal(0))
		})

		It("should clear all receivers", func() {
			nMap.newSubscriber(sID)
			nMap.newSubscriber(sID)
			Expect(nMap.len()).To(Equal(2))
			_ = nMap.close()
			Expect(nMap.len()).To(Equal(0))
		})

		It("should return error when policy is set for single subscriber", func() {
			_, err := nMap.newSubscriber(sID, SubscriberWithSelectionPolicy(selection.MakePolicy(selection.CountingWindow, 0, 0, time.Now(), time.Nanosecond, time.Nanosecond)))
			Expect(err).To(Equal(SubscriberPolicyError))
		})
	})
})
