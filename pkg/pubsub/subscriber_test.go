package pubsub

import (
	"sync"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
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

		It("should return the notify channel", func() {
			Expect(rec.Notify()).To(Equal(rec.notify))
		})

		It("should consume events", func() {
			event := events.NewEvent("test-event")
			go func() {
				rec.doNotify(event)
			}()

			received, err := rec.Consume()
			Expect(err).To(BeNil())
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
			nMap = newNotificationMap[string](MakeStreamDescription[string]("aTopic", WithAsyncReceiver(true)), ch, newStreamMetrics())
			rec = nMap.newSubscriber(streamID)
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

			received, err := rec.Consume()
			Expect(err).To(BeNil())
			Expect(received).To(Equal(event))
		})

		It("should return the notify channel", func() {
			ch := rec.Notify()
			Expect(ch).NotTo(BeNil())
		})

		It("should close resources", func() {
			rec.close()
			Eventually(func() error {
				_, err := rec.Consume()
				return err
			}).Should(HaveOccurred())
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
			nMap = newNotificationMap[string](MakeStreamDescription[string]("aTopic", WithAsyncReceiver(true), WithBufferCapacity(1)), ch, newStreamMetrics())
			rec = nMap.newSubscriber(streamID)
		})

		AfterEach(func() {
			_ = nMap.close()
		})

		It("should apply backpressure when limit is reached", func() {
			event1 := events.NewEvent("e1")
			event2 := events.NewEvent("e2")
			event3 := events.NewEvent("e3")

			rec.doNotify(event1) // Moves to notify channel (blocked there)
			rec.doNotify(event2) // Sits in buffer (limit reached)

			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				rec.doNotify(event3) // Should block
				close(done)
			}()

			Consistently(done).ShouldNot(BeClosed())

			v, err := rec.Consume()
			Expect(err).To(BeNil())
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
			nMap = newNotificationMap[string](MakeStreamDescription[string]("aTopic"), ch, newStreamMetrics())
			sID = MakeStreamID[string]("topic")
			nMap.start()
		})

		AfterEach(func() {
			_ = nMap.close()
		})

		It("should create new receivers", func() {
			rec := nMap.newSubscriber(sID)
			Expect(rec).NotTo(BeNil())
			Expect(nMap.len()).To(Equal(1))
		})

		It("should notify all receivers", func() {
			rec1 := nMap.newSubscriber(sID)
			rec2 := nMap.newSubscriber(sID)

			event := events.NewEvent("broadcast")
			go func() { ch <- event }()

			var (
				e1, e2     events.Event[string]
				err1, err2 error
				wg         sync.WaitGroup
			)

			wg.Go(func() {
				e1, err1 = rec1.Consume()
			})
			wg.Go(func() {
				e2, err2 = rec2.Consume()
			})
			wg.Wait()

			Expect(err1).To(BeNil())
			Expect(err2).To(BeNil())
			Expect(e1).To(Equal(event))
			Expect(e2).To(Equal(event))
		})

		It("should remove receivers", func() {
			rec := nMap.newSubscriber(sID)
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
	})
})
