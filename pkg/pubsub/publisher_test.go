package pubsub

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

// Mock Stream implementation for testing publishers
type mockStream[T any] struct {
	id              StreamID
	channel         events.EventChannel[T]
	publishedEvents []events.Event[T]
}

func (m *mockStream[T]) Run()                             {}
func (m *mockStream[T]) TryClose() bool                   { return true }
func (m *mockStream[T]) forceClose()                      { close(m.channel) }
func (m *mockStream[T]) HasPublishersOrSubscribers() bool { return false }
func (m *mockStream[T]) ID() StreamID                     { return m.id }
func (m *mockStream[T]) Description() StreamDescription   { return StreamDescription{} }
func (m *mockStream[T]) copyFrom(Stream)                  {}

func (m *mockStream[T]) publish(e events.Event[T]) error {
	m.publishedEvents = append(m.publishedEvents, e)
	return nil
}

func (m *mockStream[T]) subscribe() (StreamReceiver[T], error) { return nil, nil }
func (m *mockStream[T]) unsubscribe(id StreamReceiverID)       {}
func (m *mockStream[T]) newPublisher() (Publisher[T], error)   { return nil, nil }
func (m *mockStream[T]) removePublisher(id PublisherID)        {}
func (m *mockStream[T]) subscribers() *notificationMap[T]      { return nil }
func (m *mockStream[T]) publishers() publisherFanIn[T]         { return nil }
func (m *mockStream[T]) events() buffer.Buffer[T]              { return nil }

func createMockStream[T any](id StreamID) *mockStream[T] {

	s := &mockStream[T]{id: id, channel: make(events.EventChannel[T])}

	go func(newS *mockStream[T]) {
		run := true

		for run {
			e, more := <-newS.channel
			if more {
				_ = newS.publish(e)
			} else {
				run = false
			}
		}
	}(s)

	return s
}

var _ = Describe("Publisher", func() {

	Describe("PublisherID", func() {
		It("should return string representation", func() {
			uid := uuid.New()
			pid := PublisherID(uid)
			Expect(pid.String()).To(Equal(uid.String()))
		})
	})

	Describe("defaultPublisher", func() {
		var (
			streamID StreamID
			mockS    *mockStream[string]
			fanIn    publisherFanIn[string]
			pub      *defaultPublisher[string]
		)

		BeforeEach(func() {
			streamID = MakeStreamID[string]("test-stream")
			mockS = createMockStream[string](streamID)
			fanIn = newPublisherSync[string](mockS.Description(), mockS.channel)
			pub = newDefaultPublisher(streamID, fanIn)
		})

		AfterEach(func() {
			mockS.forceClose()
			mockS = nil
			pub = nil
		})

		It("should return a publisher ID", func() {
			Expect(pub.ID()).NotTo(BeNil())
		})

		It("should return correct StreamID", func() {
			Expect(pub.StreamID()).To(Equal(streamID))
		})

		It("should publish events", func() {
			e := events.NewEvent("hello")
			err := pub.Publish(e)

			Expect(err).To(BeNil())
			Eventually(func() events.Event[string] {
				evs := mockS.publishedEvents
				if len(evs) > 0 {
					return evs[0]
				}
				return events.NewEvent[string]("a")
			}).Should(Equal(e))
		})

		It("should publish content", func() {
			err := pub.PublishC("world")
			Expect(err).To(BeNil())
			Eventually(func() []events.Event[string] {
				evs := mockS.publishedEvents
				if len(evs) > 0 {
					return evs
				}
				return []events.Event[string]{}
			}).Should(HaveLen(1))
			Eventually(mockS.publishedEvents[0].GetContent()).Should(Equal("world"))
		})
	})

	Describe("emptyPublisherFanIn", func() {
		var empty emptyPublisherFanIn[string]

		It("should have length 0", func() {
			Expect(empty.len()).To(Equal(0))
		})

		It("should return error on newPublisher", func() {
			p, err := empty.newPublisher()
			Expect(p).To(BeNil())
			Expect(err).To(Equal(EmptyPublisherFanInPublisherError))
		})

		It("should do nothing on publish", func() {
			err := empty.publish(events.NewEvent("test"))
			Expect(err).To(BeNil())
		})

		It("should do nothing on publishC", func() {
			err := empty.publishC("test")
			Expect(err).To(BeNil())
		})
	})

	Describe("publisherFanInMutexSync", func() {
		var (
			mockS *mockStream[string]
			fanIn publisherFanIn[string]
		)

		BeforeEach(func() {
			mockS = createMockStream[string](MakeStreamID[string]("multi"))
			fanIn = newPublisherSync[string](mockS.Description(), mockS.channel)
		})

		AfterEach(func() {
			mockS.forceClose()
		})

		It("should allow multiple publishers", func() {
			p1, err1 := fanIn.newPublisher()
			p2, err2 := fanIn.newPublisher()
			Expect(err1).To(BeNil())
			Expect(err2).To(BeNil())
			Expect(p1).NotTo(BeNil())
			Expect(p2).NotTo(BeNil())
			Expect(fanIn.len()).To(Equal(2))
		})

		It("should remove publisher", func() {
			p1, _ := fanIn.newPublisher()
			p2, _ := fanIn.newPublisher()
			Expect(fanIn.len()).To(Equal(2))
			fanIn.remove(p1.ID())
			Expect(fanIn.len()).To(Equal(1))
			fanIn.remove(p2.ID())
			Expect(fanIn.len()).To(Equal(0))
		})

		It("should clear publishers", func() {
			p1, _ := fanIn.newPublisher()
			fanIn.Close()
			Expect(fanIn.len()).To(Equal(0))

			err := p1.PublishC("test")
			Expect(err).To(BeNil())
			Expect(mockS.publishedEvents).To(BeEmpty())
		})
	})
})
