package pubsub

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

// Mock stream implementation for testing publishers
type mockStream[T any] struct {
	id              StreamID
	channel         events.EventChannel[T]
	publishedEvents []events.Event[T]
}

func (m *mockStream[T]) subscribeBatch(opts ...SubscriberOption) (BatchSubscriber[T], error) {
	return nil, nil
}
func (m *mockStream[T]) publishC(content T) error {
	e := events.NewEvent(content)
	return m.publish(e)
}
func (m *mockStream[T]) publishers() publisherArr[T] { return nil }

func (m *mockStream[T]) clearPublishers() {}
func (m *mockStream[T]) streamMetrics() *StreamMetrics {
	return newStreamMetrics()
}
func (m *mockStream[T]) run()                                  {}
func (m *mockStream[T]) tryClose() bool                        { return true }
func (m *mockStream[T]) forceClose()                           { close(m.channel) }
func (m *mockStream[T]) hasPublishersOrSubscribers() bool      { return false }
func (m *mockStream[T]) ID() StreamID                          { return m.id }
func (m *mockStream[T]) Description() StreamDescription        { return StreamDescription{} }
func (m *mockStream[T]) migrateStream(stream)                  {}
func (m *mockStream[T]) addPublisher(pub *defaultPublisher[T]) {}

func (m *mockStream[T]) publish(e events.Event[T]) error {
	m.publishedEvents = append(m.publishedEvents, e)
	return nil
}

func (m *mockStream[T]) subscribe() (Subscriber[T], error)   { return nil, nil }
func (m *mockStream[T]) unsubscribe(id SubscriberID)         {}
func (m *mockStream[T]) newPublisher() (Publisher[T], error) { return nil, nil }
func (m *mockStream[T]) removePublisher(id PublisherID)      {}
func (m *mockStream[T]) subscribers() *notificationMap[T]    { return nil }

func (m *mockStream[T]) events() buffer.Buffer[T] { return nil }

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
			fanIn = mockS
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
			pub.Publish(e)

			Eventually(func() events.Event[string] {
				evs := mockS.publishedEvents
				if len(evs) > 0 {
					return evs[0]
				}
				return events.NewEvent[string]("a")
			}).Should(Equal(e))
		})

		It("should publish content", func() {
			pub.PublishC("world")
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

		It("should do nothing on publish", func() {
			empty.publish(events.NewEvent("test"))
		})

		It("should do nothing on publishC", func() {
			empty.publishC("test")
		})
	})
})
