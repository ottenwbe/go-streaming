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
			s, err := pubsub.GetOrAddStream[int](topic)
			defer pubsub.TryRemoveStreams(s)
			Expect(err).To(BeNil())
			Expect(s).NotTo(BeNil())
			Expect(s.Topic).To(Equal(topic))
		})

		It("returns existing stream if it exists", func() {
			topic := "get-or-add-2"
			s1, _ := pubsub.GetOrAddStream[int](topic)
			s2, err := pubsub.GetOrAddStream[int](topic)
			defer pubsub.TryRemoveStreams(s1)
			defer pubsub.TryRemoveStreams(s2)
			Expect(err).To(Equal(pubsub.StreamAlreadyExistsError))
			Expect(s1).To(Equal(s2))
		})

		It("is not replacing an existing stream if the latter should be preserved", func() {
			var topic = "test-ps-2"
			d1 := pubsub.MakeStreamDescription[string](topic)
			d2 := pubsub.MakeStreamDescription[string](topic, pubsub.WithAsynchronousStream(true))

			stream1, _ := pubsub.GetOrAddStream[string](topic)
			stream2, _ := pubsub.GetOrAddStream[string](topic, pubsub.WithAsynchronousStream(true))
			defer pubsub.TryRemoveStreams(stream1)
			defer pubsub.TryRemoveStreams(stream2)

			d, _ := pubsub.GetDescription(stream2)
			Expect(d).To(Equal(d1))
			Expect(d).ToNot(Equal(d2))
		})

		It("can add streams that are auto cleaned when no longer used", func() {
			var topic = "test-auto-clean"

			s, err := pubsub.GetOrAddStream[string](topic, pubsub.WithAutoCleanup(true))
			Expect(err).To(BeNil())

			dResult, _ := pubsub.GetDescription(s)
			Expect(dResult.AutoCleanup).To(BeTrue())

			sub, err := pubsub.SubscribeByTopic[string](topic)
			Expect(err).To(BeNil())
			err = pubsub.Unsubscribe[string](sub)
			Expect(err).To(BeNil())

			_, err = pubsub.GetDescription(s)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})

	})

	Describe("AddOrReplaceStream", func() {
		It("successfully adds a new stream if it doesn't exist", func() {
			topic := "test-ps-1"
			d := pubsub.MakeStreamDescription[string](topic)
			sID, err := pubsub.AddOrReplaceStream[string](topic)
			Expect(err).To(BeNil())

			r, e := pubsub.GetDescription(sID)
			Expect(r).To(Equal(d))
			Expect(e).To(BeNil())
		})
		//		It("registers a stream from yml if it does not exist", func() {
		//			var yml = `
		//id:
		//  topic: 3c191d62-6574-4951-a9e6-4ec83c947250
		//  type: map[string]interface{}
		//asyncStream: true
		//`
		//			d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
		//			streamID, err := pubsub.AddOrReplaceStream[map[string]interface{}](d)
		//			Expect(err).To(BeNil())
		//			Expect(streamID).To(Equal(d.StreamID()))
		//
		//			_, err = pubsub.GetDescription(d.StreamID())
		//			Expect(err).To(BeNil())
		//
		//		})
		It("allows for two streams with same name but different types to exist", func() {

			topic := "same"

			s1ID, _ := pubsub.AddOrReplaceStream[int](topic)
			s2ID, _ := pubsub.AddOrReplaceStream[float64](topic, pubsub.WithAsynchronousStream(true))
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

			s1ID, _ := pubsub.AddOrReplaceStream[int](topic)
			s2ID, _ := pubsub.AddOrReplaceStream[int](topic, pubsub.WithAsynchronousStream(true))
			defer pubsub.TryRemoveStreams(s1ID)
			defer pubsub.TryRemoveStreams(s2ID)

			r1, err1 := pubsub.GetDescription(s1ID)
			_, err2 := pubsub.GetDescription(s2ID)

			Expect(err1).To(BeNil())
			Expect(err2).To(BeNil())
			Expect(r1.Asynchronous).To(BeTrue())
		})
	})

	//	Describe("ForceRemoveStream", func() {
	//		It("removes stream from pub sub system", func() {
	//			var yml = `
	//id:
	//  topic: 4c191d62-6574-4951-a9e6-4ec83c947250
	//  type: map[string]interface{}
	//asyncStream: true
	//`
	//			d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
	//			id, err := pubsub.AddOrReplaceStream[map[string]interface{}](d)
	//
	//			pubsub.ForceRemoveStream(id)
	//
	//			_, err = pubsub.GetDescription(d.StreamID())
	//			Expect(err).To(Equal(pubsub.StreamNotFoundError))
	//		})
	//	})

	Describe("TryRemoveStreams", func() {
		It("is successful if stream still has no subscribers/publishers", func() {

			sID, err := pubsub.AddOrReplaceStream[int]("try-close-1")
			pubsub.TryRemoveStreams(sID)

			_, err = pubsub.GetDescription(sID)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})
		It("is not successful if stream still has publishers", func() {
			sID, err := pubsub.AddOrReplaceStream[int]("try-close-3")
			defer pubsub.ForceRemoveStream(sID)

			pubsub.RegisterPublisher[int](sID)

			pubsub.TryRemoveStreams(sID)

			_, err = pubsub.GetDescription(sID)
			Expect(err).To(BeNil())
		})
		It("is not successful if stream still has subscribers", func() {

			s, err := pubsub.AddOrReplaceStream[int]("try-close-2")
			Expect(err).To(BeNil())
			pubsub.SubscribeByTopicID[int](s)

			pubsub.TryRemoveStreams(s)
			defer pubsub.ForceRemoveStream(s)

			_, err = pubsub.GetDescription(s)
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

			sID, _ := pubsub.GetOrAddStream[string](topic)

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

			id, _ := pubsub.AddOrReplaceStream[string](topic)
			defer pubsub.ForceRemoveStream(id)

			rec, _ := pubsub.SubscribeByTopicID[string](id)

			event := "test 1"
			go func() {
				publisher, _ := pubsub.RegisterPublisher[string](id)
				publisher.Publish(event)
			}()
			eResult, _ := rec.Next()

			Expect(event).To(Equal(eResult.GetContent()))
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
			sID, err := pubsub.AddOrReplaceStream[int](topic)
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
			sID, err := pubsub.AddOrReplaceStream[int](topic)
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

			sID, err := pubsub.AddOrReplaceStream[int](topic)
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

			sID, err := pubsub.AddOrReplaceStream[int](topic)
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

			sID, err := pubsub.AddOrReplaceStream[int](topic)
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

			pub.Publish(1)

			m, err := pubsub.Metrics(id)
			Expect(err).To(BeNil())
			Expect(m).ToNot(BeNil())
			Eventually(m.NumEventsIn()).Should(Equal(uint64(1)))
			Eventually(m.NumEventsOut()).Should(Equal(uint64(1)))
		})
	})

})
