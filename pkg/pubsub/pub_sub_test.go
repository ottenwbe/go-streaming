package pubsub_test

import (
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PubSub", func() {

	var repo *pubsub.StreamRepository

	BeforeEach(func() {
		repo = pubsub.NewStreamRepository()
	})

	Describe("GetOrAddStream", func() {
		It("adds a new stream if it doesn't exist", func() {
			topic := "get-or-add-1"
			s, err := pubsub.GetOrAddStreamOnRepository[int](repo, topic)
			defer repo.TryRemoveStreams(s)
			Expect(err).To(BeNil())
			Expect(s).NotTo(BeNil())
			Expect(s.Topic).To(Equal(topic))
		})

		It("returns existing stream if it exists", func() {
			topic := "get-or-add-2"
			s1, _ := pubsub.GetOrAddStreamOnRepository[int](repo, topic)
			s2, err := pubsub.GetOrAddStreamOnRepository[int](repo, topic)
			defer repo.TryRemoveStreams(s1)
			defer repo.TryRemoveStreams(s2)
			Expect(err).To(Equal(pubsub.StreamAlreadyExistsError))
			Expect(s1).To(Equal(s2))
		})

		It("is not replacing an existing stream if the latter should be preserved", func() {
			var topic = "test-ps-2"
			d1 := pubsub.MakeStreamDescription[string](topic)
			d2 := pubsub.MakeStreamDescription[string](topic, pubsub.WithAsynchronousStream(true))

			stream1, _ := pubsub.GetOrAddStreamOnRepository[string](repo, topic)
			stream2, _ := pubsub.GetOrAddStreamOnRepository[string](repo, topic, pubsub.WithAsynchronousStream(true))
			defer repo.TryRemoveStreams(stream1)
			defer repo.TryRemoveStreams(stream2)

			d, _ := repo.GetDescription(stream2)
			Expect(d).To(Equal(d1))
			Expect(d).ToNot(Equal(d2))
		})

		It("can add streams that are auto cleaned when no longer used", func() {
			var topic = "test-auto-clean"

			s, err := pubsub.GetOrAddStreamOnRepository[string](repo, topic, pubsub.WithAutoCleanup(true))
			Expect(err).To(BeNil())

			dResult, _ := repo.GetDescription(s)
			Expect(dResult.AutoCleanup).To(BeTrue())

			sub, err := pubsub.SubscribeByTopicOnRepository[string](repo, topic, func(_ events.Event[string]) {})
			Expect(err).To(BeNil())
			err = pubsub.UnsubscribeOnRepository[string](repo, sub)
			Expect(err).To(BeNil())

			_, err = repo.GetDescription(s)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})

	})

	Describe("AddOrReplaceStream", func() {
		It("successfully adds a new stream if it doesn't exist", func() {
			topic := "test-ps-1"
			d := pubsub.MakeStreamDescription[string](topic)
			sID, err := pubsub.AddOrReplaceStreamOnRepository[string](repo, topic)
			Expect(err).To(BeNil())

			r, e := repo.GetDescription(sID)
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

			s1ID, _ := pubsub.AddOrReplaceStreamOnRepository[int](repo, topic)
			s2ID, _ := pubsub.AddOrReplaceStreamOnRepository[float64](repo, topic, pubsub.WithAsynchronousStream(true))
			defer repo.ForceRemoveStream(s1ID)
			defer repo.ForceRemoveStream(s2ID)

			r1, err1 := repo.GetDescription(s1ID)
			r2, err2 := repo.GetDescription(s2ID)

			Expect(err1).To(BeNil())
			Expect(err2).To(BeNil())
			Expect(r1.ID.Topic).To(Equal(r2.ID.Topic))
			Expect(r1.ID.TopicType).ToNot(Equal(r2.ID.TopicType))

		})
		It("supports replacing streams", func() {
			topic := "streamA"

			s1ID, _ := pubsub.AddOrReplaceStreamOnRepository[int](repo, topic)
			s2ID, _ := pubsub.AddOrReplaceStreamOnRepository[int](repo, topic, pubsub.WithAsynchronousStream(true))
			defer repo.TryRemoveStreams(s1ID)
			defer repo.TryRemoveStreams(s2ID)

			r1, err1 := repo.GetDescription(s1ID)
			_, err2 := repo.GetDescription(s2ID)

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

			sID, err := pubsub.AddOrReplaceStreamOnRepository[int](repo, "try-close-1")
			repo.TryRemoveStreams(sID)

			_, err = repo.GetDescription(sID)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})
		It("is not successful if stream still has publishers", func() {
			sID, err := pubsub.AddOrReplaceStreamOnRepository[int](repo, "try-close-3")
			defer repo.ForceRemoveStream(sID)

			pubsub.RegisterPublisherOnRepository[int](repo, sID)

			repo.TryRemoveStreams(sID)

			_, err = repo.GetDescription(sID)
			Expect(err).To(BeNil())
		})
		It("is not successful if stream still has subscribers", func() {

			s, err := pubsub.AddOrReplaceStreamOnRepository[int](repo, "try-close-2")
			Expect(err).To(BeNil())
			pubsub.SubscribeByTopicIDOnRepository[int](repo, s, func(_ events.Event[int]) {})

			repo.TryRemoveStreams(s)
			defer repo.ForceRemoveStream(s)

			_, err = repo.GetDescription(s)
			Expect(err).To(BeNil())
		})
	})

	Describe("GetDescription", func() {
		It("results in an error if non-existing", func() {
			id := pubsub.RandomStreamID()
			_, e := repo.GetDescription(id)
			Expect(e).NotTo(BeNil())
		})
	})

	Describe("Manual Start", func() {
		It("should not start automatically if AutoStart is false", func() {
			topic := "manual-start-topic"
			sID, err := pubsub.AddOrReplaceStreamOnRepository[int](repo, topic, pubsub.WithAutoStart(false))
			Expect(err).To(BeNil())
			err = repo.StartStream(sID)
			Expect(err).To(BeNil())
		})
	})

	Describe("Unsubscribe", func() {
		It("is successful when the stream exists", func() {
			var topic = "test-unsub-1"

			sID, _ := pubsub.GetOrAddStreamOnRepository[string](repo, topic)

			rec, _ := pubsub.SubscribeByTopicIDOnRepository[string](repo, sID, func(_ events.Event[string]) {})

			Expect(func() { _ = pubsub.UnsubscribeOnRepository[string](repo, rec) }).NotTo(Panic())
		})
		It("from non existing stream ends up in no error", func() {

			Expect(func() { _ = pubsub.UnsubscribeOnRepository[string](repo, nil) }).NotTo(Panic())
		})
	})

	Describe("Publish", func() {
		It("allows to send and receive events via the pub sub system", func() {
			var topic = "test-send-rec-1"

			id, _ := pubsub.AddOrReplaceStreamOnRepository[string](repo, topic)
			defer repo.ForceRemoveStream(id)

			event := "test 1"
			done := make(chan struct{})
			var result events.Event[string]

			rec, _ := pubsub.SubscribeByTopicIDOnRepository[string](repo, id, func(e events.Event[string]) {
				result = e
				close(done)
			})
			defer pubsub.UnsubscribeOnRepository[string](repo, rec)

			go func() {
				publisher, _ := pubsub.RegisterPublisherOnRepository[string](repo, id)
				publisher.Publish(event)
			}()

			Eventually(done).Should(BeClosed())
			Expect(event).To(Equal(result.GetContent()))
		})
	})

	Describe("InstantPublishByTopic", func() {
		It("succeeds if stream does not exist (auto-create)", func() {
			err := pubsub.InstantPublishByTopicOnRepository[events.Event[string]](repo, "non-existent-topic", events.NewEvent("hello"))
			Expect(err).To(BeNil())
		})
	})

	Describe("RegisterPublisher", func() {
		It("registers a publisher for an existing stream", func() {
			topic := "register-pub-1"
			sID, err := pubsub.AddOrReplaceStreamOnRepository[int](repo, topic)
			Expect(err).To(BeNil())
			defer repo.ForceRemoveStream(sID)

			pub, err := pubsub.RegisterPublisherOnRepository[int](repo, sID)
			Expect(err).To(BeNil())
			Expect(pub).NotTo(BeNil())
			Expect(pub.StreamID()).To(Equal(sID))
		})
		It("creates stream if stream does not exist", func() {
			id := pubsub.RandomStreamID()
			pub, err := pubsub.RegisterPublisherOnRepository[int](repo, id)
			Expect(err).To(BeNil())
			Expect(pub).NotTo(BeNil())
			pubsub.UnRegisterPublisherOnRepository[int](repo, pub)
		})
	})

	Describe("RegisterPublisherByTopic", func() {
		It("registers a publisher for an existing stream by topic", func() {
			topic := "register-pub-topic-1"
			sID, err := pubsub.AddOrReplaceStreamOnRepository[int](repo, topic)
			Expect(err).To(BeNil())
			defer repo.ForceRemoveStream(sID)

			pub, err := pubsub.RegisterPublisherByTopicOnRepository[int](repo, topic)
			Expect(err).To(BeNil())
			Expect(pub).NotTo(BeNil())
			Expect(pub.StreamID()).To(Equal(sID))
		})
		It("creates stream if stream does not exist", func() {
			pub, err := pubsub.RegisterPublisherByTopicOnRepository[int](repo, "non-existent-topic-pub")
			Expect(err).To(BeNil())
			Expect(pub).NotTo(BeNil())
			pubsub.UnRegisterPublisherOnRepository[int](repo, pub)
		})
	})

	Describe("UnRegisterPublisher", func() {
		It("handles nil publisher gracefully", func() {
			err := pubsub.UnRegisterPublisherOnRepository[int](repo, nil)
			Expect(err).To(BeNil())
		})

		It("unregisters an existing publisher successfully", func() {
			topic := "unregister-pub-1"

			sID, err := pubsub.AddOrReplaceStreamOnRepository[int](repo, topic)
			Expect(err).To(BeNil())
			defer repo.ForceRemoveStream(sID)

			pub, err := pubsub.RegisterPublisherOnRepository[int](repo, sID)
			Expect(err).To(BeNil())

			err = pubsub.UnRegisterPublisherOnRepository[int](repo, pub)
			Expect(err).To(BeNil())

			repo.TryRemoveStreams(sID)
			_, err = repo.GetDescription(sID)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})

		It("returns error when stream does not exist", func() {
			topic := "unregister-pub-2"

			sID, err := pubsub.AddOrReplaceStreamOnRepository[int](repo, topic)
			Expect(err).To(BeNil())

			pub, err := pubsub.RegisterPublisherOnRepository[int](repo, sID)
			Expect(err).To(BeNil())

			repo.ForceRemoveStream(sID)

			err = pubsub.UnRegisterPublisherOnRepository[int](repo, pub)
			Expect(err).To(Equal(pubsub.StreamNotFoundError))
		})
	})

	Describe("SubscribeByTopic", func() {
		It("subscribes to an existing stream", func() {
			topic := "subscribe-by-topic-1"

			sID, err := pubsub.AddOrReplaceStream[int](topic)
			Expect(err).To(BeNil())
			defer pubsub.ForceRemoveStream(sID)

			rec, err := pubsub.SubscribeByTopic[int](topic, func(_ events.Event[int]) {})
			Expect(err).To(BeNil())
			Expect(rec).NotTo(BeNil())
			Expect(rec.StreamID().Topic).To(Equal(topic))
		})
		It("creates stream if stream does not exist", func() {
			rec, err := pubsub.SubscribeByTopic[int]("non-existent-topic-sub", func(_ events.Event[int]) {})
			Expect(err).To(BeNil())
			Expect(rec).NotTo(BeNil())
			pubsub.Unsubscribe(rec)
		})
		It("creates stream if non existing in the pub sub system", func() {
			id := pubsub.RandomStreamID()
			rec, e := pubsub.SubscribeByTopicID[int](id, func(_ events.Event[int]) {})
			Expect(e).To(BeNil())
			Expect(rec).NotTo(BeNil())
			pubsub.Unsubscribe(rec)
		})
	})

	Describe("Metrics", func() {
		It("handles NilStreamID gracefully", func() {
			m, err := repo.Metrics(pubsub.NilStreamID())
			Expect(err).ToNot(BeNil())
			Expect(m).ToNot(BeNil())
			Expect(m.NumEventsIn()).To(Equal(uint64(0)))
			Expect(m.NumEventsOut()).To(Equal(uint64(0)))
		})

		It("returns accurate metrics for a stream", func() {

			id := pubsub.MakeStreamID[int]("metrics-topic")

			pub, err := pubsub.RegisterPublisherOnRepository[int](repo, id)
			defer pubsub.UnRegisterPublisherOnRepository[int](repo, pub)

			Expect(err).To(BeNil())

			pub.Publish(1)

			m, err := repo.Metrics(id)
			Expect(err).To(BeNil())
			Expect(m).ToNot(BeNil())
			Eventually(m.NumEventsIn()).Should(Equal(uint64(1)))
			Eventually(m.NumEventsOut()).Should(Equal(uint64(1)))
		})
	})

})
