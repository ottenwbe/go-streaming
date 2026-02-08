package pubsub_test

import (
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PubSub", func() {

	Describe("GetOrAddStream", func() {
		It("adds a new stream if it doesn't exist", func() {
			topic := "get-or-add-1"
			desc := pubsub.MakeStreamDescription[int](topic)
			s, err := pubsub.GetOrAddStream[int](desc)
			Expect(err).To(BeNil())
			Expect(s).NotTo(BeNil())
			Expect(s.Topic).To(Equal(topic))
			pubsub.TryRemoveStreams(s)
		})

		It("returns existing stream if it exists", func() {
			topic := "get-or-add-2"
			desc := pubsub.MakeStreamDescription[int](topic)
			s1, _ := pubsub.GetOrAddStream[int](desc)
			s2, err := pubsub.GetOrAddStream[int](desc)
			defer pubsub.TryRemoveStreams(s1)
			defer pubsub.TryRemoveStreams(s2)
			Expect(err).To(BeNil())
			Expect(s1).To(Equal(s2))
		})

		It("is not replacing an existing stream if the latter should be preserved", func() {
			var topic = "test-ps-2"
			d1 := pubsub.MakeStreamDescription[string](topic)
			d2 := pubsub.MakeStreamDescription[string](topic, pubsub.WithAsyncStream(true))

			stream1, _ := pubsub.GetOrAddStream[string](d1)
			stream2, _ := pubsub.GetOrAddStream[string](d2)
			defer pubsub.TryRemoveStreams(stream1)
			defer pubsub.TryRemoveStreams(stream2)

			d, _ := pubsub.GetDescription(stream2)
			Expect(d).To(Equal(d1))
			Expect(d).ToNot(Equal(d2))
		})

		It("can add streams that are auto cleaned when no longer used", func() {
			var topic = "test-auto-clean"
			d := pubsub.MakeStreamDescription[string](topic, pubsub.WithAutoCleanup(true))

			s, err := pubsub.GetOrAddStream[string](d)
			Expect(err).To(BeNil())

			dResult, _ := pubsub.GetDescription(s)
			Expect(dResult.AutoCleanup).To(BeTrue())

			sub, err := pubsub.SubscribeByTopic[string](topic)
			Expect(err).To(BeNil())
			err = pubsub.Unsubscribe[string](sub)
			Expect(err).To(BeNil())

			_, err = pubsub.GetDescription(d.ID)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})

	})

	Describe("AddOrReplaceStreamFromDescription", func() {
		It("successfully adds a new stream if it doesn't exist", func() {
			topic := "test-ps-1"
			d := pubsub.MakeStreamDescription[string](topic)
			sID, err := pubsub.AddOrReplaceStreamFromDescription[string](d)
			Expect(err).To(BeNil())

			r, e := pubsub.GetDescription(sID)
			Expect(r).To(Equal(d))
			Expect(e).To(BeNil())
		})
		It("registers a stream from yml if it does not exist", func() {
			var yml = `
id: 
  topic: 3c191d62-6574-4951-a9e6-4ec83c947250
  type: map[string]interface{}
asyncStream: true
`
			d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
			streamID, err := pubsub.AddOrReplaceStreamFromDescription[map[string]interface{}](d)
			Expect(err).To(BeNil())
			Expect(streamID).To(Equal(d.StreamID()))

			_, err = pubsub.GetDescription(d.StreamID())
			Expect(err).To(BeNil())

		})
		It("is NOT successful when the stream id is invalid", func() {

			s1 := pubsub.MakeStreamDescriptionFromID(pubsub.NilStreamID())
			_, err := pubsub.AddOrReplaceStreamFromDescription[string](s1)

			Expect(err).To(Equal(pubsub.StreamIDNilError))
		})
		It("allows for two streams with same name but different types to exist", func() {
			s1 := pubsub.MakeStreamDescription[int]("same")
			s2 := pubsub.MakeStreamDescription[float64]("same", pubsub.WithAsyncStream(true))

			s1ID, _ := pubsub.AddOrReplaceStreamFromDescription[int](s1)
			s2ID, _ := pubsub.AddOrReplaceStreamFromDescription[float64](s2)
			defer pubsub.ForceRemoveStream(s1ID)
			defer pubsub.ForceRemoveStream(s2ID)

			r1, err1 := pubsub.GetDescription(s1ID)
			r2, err2 := pubsub.GetDescription(s2ID)

			Expect(err1).To(BeNil())
			Expect(err2).To(BeNil())
			Expect(r1.ID.Topic).To(Equal(r2.ID.Topic))
			Expect(r1.ID.TopicType).ToNot(Equal(r2.ID.TopicType))

		})
		It("supports replacing streams", func() {
			topic := "streamA"
			s1 := pubsub.MakeStreamDescription[int](topic)
			s2 := pubsub.MakeStreamDescription[int](topic, pubsub.WithAsyncStream(true))

			s1ID, _ := pubsub.AddOrReplaceStreamFromDescription[int](s1)
			s2ID, _ := pubsub.AddOrReplaceStreamFromDescription[int](s2)
			defer pubsub.ForceRemoveStream(s1ID)
			defer pubsub.ForceRemoveStream(s2ID)

			r1, err1 := pubsub.GetDescription(s1ID)
			_, err2 := pubsub.GetDescription(s2ID)

			Expect(err1).To(BeNil())
			Expect(err2).To(BeNil())
			Expect(r1.AsyncStream).To(BeTrue())
		})
	})

	Describe("ForceRemoveStream", func() {
		It("removes stream from pub sub system", func() {
			var yml = `
id: 
  topic: 4c191d62-6574-4951-a9e6-4ec83c947250
  type: map[string]interface{}
asyncStream: true
`
			d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
			id, err := pubsub.AddOrReplaceStreamFromDescription[map[string]interface{}](d)

			pubsub.ForceRemoveStream(id)

			_, err = pubsub.GetDescription(d.StreamID())
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})
	})

	Describe("TryRemoveStreams", func() {
		It("is successful if stream still has no subscribers/publishers", func() {
			d := pubsub.MakeStreamDescription[int]("try-close-1")
			sID, err := pubsub.AddOrReplaceStreamFromDescription[map[string]interface{}](d)
			defer pubsub.ForceRemoveStream(sID)

			pubsub.AddOrReplaceStreamFromDescription[int](d)
			pubsub.TryRemoveStreams(sID)

			_, err = pubsub.GetDescription(d.StreamID())
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})
		It("is not successful if stream still has publishers", func() {
			d := pubsub.MakeStreamDescription[int]("try-close-3")
			sID, err := pubsub.AddOrReplaceStreamFromDescription[int](d)
			defer pubsub.ForceRemoveStream(sID)

			pubsub.AddOrReplaceStreamFromDescription[int](d)
			pubsub.RegisterPublisher[int](d.StreamID())

			pubsub.TryRemoveStreams(sID)

			_, err = pubsub.GetDescription(d.StreamID())
			Expect(err).To(BeNil())
		})
		It("is not successful if stream still has subscribers", func() {
			d := pubsub.MakeStreamDescription[int]("try-close-2")
			s, err := pubsub.AddOrReplaceStreamFromDescription[map[string]interface{}](d)
			defer pubsub.ForceRemoveStream(s)

			pubsub.AddOrReplaceStreamFromDescription[int](d)
			pubsub.SubscribeByTopicID[int](d.StreamID())

			pubsub.TryRemoveStreams(s)

			_, err = pubsub.GetDescription(d.StreamID())
			Expect(err).To(BeNil())
		})
	})

	Describe("GetDescription", func() {
		It("results in an error if non-existing", func() {
			id := pubsub.RandomStreamID()
			_, e := pubsub.GetDescription(id)
			Expect(e).NotTo(BeNil())
		})
	})

	Describe("Unsubscribe", func() {
		It("is successful when the stream exists", func() {
			var topic = "test-unsub-1"
			s := pubsub.MakeStreamDescription[string](topic)
			sID, _ := pubsub.GetOrAddStream[string](s)

			rec, _ := pubsub.SubscribeByTopicID[string](sID)

			Expect(func() { _ = pubsub.Unsubscribe(rec) }).NotTo(Panic())
		})
		It("from non existing stream ends up in no error", func() {

			Expect(func() { _ = pubsub.Unsubscribe[string](nil) }).NotTo(Panic())
		})
	})

	Describe("Publish", func() {
		It("allows to send and receive events via the pub sub system", func() {
			var topic = "test-send-rec-1"
			s := pubsub.MakeStreamDescription[string](topic)
			id := s.ID

			pubsub.AddOrReplaceStreamFromDescription[string](s)
			defer pubsub.ForceRemoveStream(s.ID)

			rec, _ := pubsub.SubscribeByTopicID[string](id)

			e1 := events.NewEvent("test 1")
			go func() {
				publisher, _ := pubsub.RegisterPublisher[string](s.ID)
				publisher.Publish(e1)
			}()
			eResult := <-rec.Notify()

			Expect(e1).To(Equal(eResult))
		})
	})

	Describe("InstantPublishByTopic", func() {
		It("succeeds if stream does not exist (auto-create)", func() {
			err := pubsub.InstantPublishByTopic("non-existent-topic", events.NewEvent("hello"))
			Expect(err).To(BeNil())
		})
	})

	Describe("RegisterPublisher", func() {
		It("registers a publisher for an existing stream", func() {
			topic := "register-pub-1"
			desc := pubsub.MakeStreamDescription[int](topic)
			sID, err := pubsub.AddOrReplaceStreamFromDescription[int](desc)
			Expect(err).To(BeNil())
			defer pubsub.ForceRemoveStream(sID)

			pub, err := pubsub.RegisterPublisher[int](sID)
			Expect(err).To(BeNil())
			Expect(pub).NotTo(BeNil())
			Expect(pub.StreamID()).To(Equal(sID))
		})
		It("creates stream if stream does not exist", func() {
			id := pubsub.RandomStreamID()
			pub, err := pubsub.RegisterPublisher[int](id)
			Expect(err).To(BeNil())
			Expect(pub).NotTo(BeNil())
			pubsub.UnRegisterPublisher(pub)
		})
	})

	Describe("RegisterPublisherByTopic", func() {
		It("registers a publisher for an existing stream by topic", func() {
			topic := "register-pub-topic-1"
			desc := pubsub.MakeStreamDescription[int](topic)
			sID, err := pubsub.AddOrReplaceStreamFromDescription[int](desc)
			Expect(err).To(BeNil())
			defer pubsub.ForceRemoveStream(sID)

			pub, err := pubsub.RegisterPublisherByTopic[int](topic)
			Expect(err).To(BeNil())
			Expect(pub).NotTo(BeNil())
			Expect(pub.StreamID()).To(Equal(sID))
		})
		It("creates stream if stream does not exist", func() {
			pub, err := pubsub.RegisterPublisherByTopic[int]("non-existent-topic-pub")
			Expect(err).To(BeNil())
			Expect(pub).NotTo(BeNil())
			pubsub.UnRegisterPublisher(pub)
		})
	})

	Describe("UnRegisterPublisher", func() {
		It("handles nil publisher gracefully", func() {
			err := pubsub.UnRegisterPublisher[int](nil)
			Expect(err).To(BeNil())
		})

		It("unregisters an existing publisher successfully", func() {
			topic := "unregister-pub-1"
			desc := pubsub.MakeStreamDescription[int](topic)
			sID, err := pubsub.AddOrReplaceStreamFromDescription[int](desc)
			Expect(err).To(BeNil())
			defer pubsub.ForceRemoveStream(sID)

			pub, err := pubsub.RegisterPublisher[int](sID)
			Expect(err).To(BeNil())

			err = pubsub.UnRegisterPublisher[int](pub)
			Expect(err).To(BeNil())

			pubsub.TryRemoveStreams(sID)
			_, err = pubsub.GetDescription(sID)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})

		It("returns error when stream does not exist", func() {
			topic := "unregister-pub-2"
			desc := pubsub.MakeStreamDescription[int](topic)
			sID, err := pubsub.AddOrReplaceStreamFromDescription[int](desc)
			Expect(err).To(BeNil())

			pub, err := pubsub.RegisterPublisher[int](sID)
			Expect(err).To(BeNil())

			pubsub.ForceRemoveStream(sID)

			err = pubsub.UnRegisterPublisher[int](pub)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})
	})

	Describe("SubscribeByTopic", func() {
		It("subscribes to an existing stream", func() {
			topic := "subscribe-by-topic-1"
			desc := pubsub.MakeStreamDescription[int](topic)
			sID, err := pubsub.AddOrReplaceStreamFromDescription[int](desc)
			Expect(err).To(BeNil())
			defer pubsub.ForceRemoveStream(sID)

			rec, err := pubsub.SubscribeByTopic[int](topic)
			Expect(err).To(BeNil())
			Expect(rec).NotTo(BeNil())
			Expect(rec.StreamID().Topic).To(Equal(topic))
		})
		It("creates stream if stream does not exist", func() {
			rec, err := pubsub.SubscribeByTopic[int]("non-existent-topic-sub")
			Expect(err).To(BeNil())
			Expect(rec).NotTo(BeNil())
			pubsub.Unsubscribe(rec)
		})
		It("creates stream if non existing in the pub sub system", func() {
			id := pubsub.RandomStreamID()
			rec, e := pubsub.SubscribeByTopicID[int](id)
			Expect(e).To(BeNil())
			Expect(rec).NotTo(BeNil())
			pubsub.Unsubscribe(rec)
		})
	})

	Describe("Metrics", func() {
		It("handles NilStreamID gracefully", func() {
			m, err := pubsub.Metrics(pubsub.NilStreamID())
			Expect(err).ToNot(BeNil())
			Expect(m).ToNot(BeNil())
			Expect(m.NumEventsIn()).To(Equal(uint64(0)))
			Expect(m.NumEventsOut()).To(Equal(uint64(0)))
		})

		It("returns accurate metrics for a stream", func() {

			id := pubsub.MakeStreamID[int]("metrics-topic")

			pub, err := pubsub.RegisterPublisher[int](id)
			defer pubsub.UnRegisterPublisher(pub)

			Expect(err).To(BeNil())

			pub.PublishC(1)

			m, err := pubsub.Metrics(id)
			Expect(err).To(BeNil())
			Expect(m).ToNot(BeNil())
			Eventually(m.NumEventsIn()).Should(Equal(uint64(1)))
			Eventually(m.NumEventsOut()).Should(Equal(uint64(1)))
		})
	})

})
