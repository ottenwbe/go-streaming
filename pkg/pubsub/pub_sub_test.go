package pubsub_test

import (
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PubSub", func() {
	Describe("PubSub System", func() {

		Describe("Get Stream by ID or Topic", func() {

			var yml = `
id: 
  topic: 3c191d62-6575-4951-a9e6-4ec83c947251
  type: int
async: true
`
			d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
			pubsub.AddOrReplaceStreamFromDescription[int](d)

			It("retrieves the stream by id", func() {
				r, err := pubsub.GetStream(d.ID)
				Expect(err).To(BeNil())
				Expect(r.Description()).To(Equal(d))
			})
			It("retrieves the stream by topic", func() {
				r, err := pubsub.GetStreamByTopic[int](d.ID.Topic)
				Expect(err).To(BeNil())
				Expect(r.Description()).To(Equal(d))
			})
			It("not retrieves the stream when it does not exist (by id)", func() {
				_, err := pubsub.GetStream(pubsub.NilStreamID())
				Expect(err).To(Equal(pubsub.StreamNotFoundError))
			})
			It("not retrieves the stream when it does not exist (by topic)", func() {
				_, err := pubsub.GetStreamByTopic[int]("not-existing-name")
				Expect(err).To(Equal(pubsub.StreamNotFoundError))
			})
		})

		Describe("Create with Description", func() {
			It("registers a Stream", func() {
				var yml = `
id: 
  topic: 3c191d62-6574-4951-a9e6-4ec83c947250
  type: map[string]interface{}
async: true
`
				d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
				pubsub.AddOrReplaceStreamFromDescription[map[string]interface{}](d)

				_, err := pubsub.GetStream(d.StreamID())
				Expect(err).To(BeNil())

			})
		})

		Describe("Forcefully closing a stream", func() {
			It("removes stream from pub sub system", func() {
				var yml = `
id: 
  topic: 4c191d62-6574-4951-a9e6-4ec83c947250
  type: map[string]interface{}
async: true
`
				d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
				s, err := pubsub.AddOrReplaceStreamFromDescription[map[string]interface{}](d)
				s.Run()

				pubsub.ForceRemoveStream(s.Description())

				_, err = pubsub.GetStream(d.StreamID())
				Expect(err).To(Equal(pubsub.StreamNotFoundError))
			})
		})

		Describe("Try closing a stream", func() {
			It("is successful if stream still has no subscribers/publishers", func() {
				d := pubsub.MakeStreamDescription[int]("try-close-1", false, false)
				s, err := pubsub.AddOrReplaceStreamFromDescription[map[string]interface{}](d)
				s.Run()
				defer pubsub.ForceRemoveStream(s.Description())

				pubsub.AddOrReplaceStreamFromDescription[int](d)
				pubsub.TryRemoveStreams(s)

				_, err = pubsub.GetStream(d.StreamID())
				Expect(err).To(Equal(pubsub.StreamNotFoundError))
			})
			It("is not successful if stream still has publishers", func() {
				d := pubsub.MakeStreamDescription[int]("try-close-3", false, false)
				s, err := pubsub.AddOrReplaceStreamFromDescription[int](d)
				s.Run()
				defer pubsub.ForceRemoveStream(s.Description())

				pubsub.AddOrReplaceStreamFromDescription[int](d)
				pubsub.RegisterPublisher[int](d.StreamID())

				pubsub.TryRemoveStreams(s)

				_, err = pubsub.GetStream(d.StreamID())
				Expect(err).To(BeNil())
			})
			It("is not successful if stream still has subscribers", func() {
				d := pubsub.MakeStreamDescription[int]("try-close-2", false, false)
				s, err := pubsub.AddOrReplaceStreamFromDescription[map[string]interface{}](d)
				s.Run()
				defer pubsub.ForceRemoveStream(s.Description())

				pubsub.AddOrReplaceStreamFromDescription[int](d)
				pubsub.SubscribeByTopicID[int](d.StreamID())

				pubsub.TryRemoveStreams(s)

				_, err = pubsub.GetStream(d.StreamID())
				Expect(err).To(BeNil())
			})
		})

		Context("Adding new Stream", func() {
			It("is successful", func() {
				var topic = "test-ps-1"
				s := pubsub.NewStreamFromDescription[string](pubsub.MakeStreamDescription[string](topic, false, false))

				_ = pubsub.AddOrReplaceStream(s)

				r, e := pubsub.GetStreamByTopic[string](topic)
				Expect(r).To(Equal(s))
				Expect(e).To(BeNil())
			})
			It("is not successful if an existing stream should be preserved", func() {
				var topic = "test-ps-2"
				s1 := pubsub.NewStreamFromDescription[string](pubsub.MakeStreamDescription[string](topic, false, false))
				s2 := pubsub.NewStreamFromDescription[string](pubsub.MakeStreamDescription[string](topic, true, false))

				pubsub.GetOrAddStreams(s1)
				sResult := pubsub.GetOrAddStreams(s2)

				Expect(sResult[0].Description()).To(Equal(s1.Description()))
				Expect(sResult[0].Description()).ToNot(Equal(s2.Description()))
			})
			It("is NOT successful when the stream id is invalid", func() {

				s1 := pubsub.NewStreamFromDescription[map[string]interface{}](pubsub.MakeStreamDescriptionFromID(pubsub.NilStreamID(), false, false))
				err := pubsub.AddOrReplaceStream(s1)

				Expect(err).To(Equal(pubsub.StreamIDNilError))
			})
		})
		Context("getting stream from pubsub system", func() {
			It("results in an error if non-existing", func() {
				id := pubsub.RandomStreamID()
				_, e := pubsub.GetStream(id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("subscribing to a non existing stream", func() {
			It("ends up in an error", func() {
				id := pubsub.RandomStreamID()
				_, e := pubsub.SubscribeByTopicID[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("unsub from a stream", func() {
			It("is successful when the stream exists", func() {
				var topic = "test-unsub-1"
				s := pubsub.NewStreamFromDescription[string](pubsub.MakeStreamDescription[string](topic, false, false))
				pubsub.GetOrAddStreams(s)

				rec, _ := pubsub.SubscribeByTopicID[string](s.ID())

				Expect(func() { _ = pubsub.Unsubscribe(rec) }).NotTo(Panic())
			})
		})
		Context("unsub from non existing stream", func() {
			It("ends up in no error", func() {

				Expect(func() { _ = pubsub.Unsubscribe[string](nil) }).NotTo(Panic())
			})
		})
		Context("streams with same name and different types", func() {
			It("can exist", func() {
				s1 := pubsub.NewStreamFromDescription[int](pubsub.MakeStreamDescription[int]("same", false, false))
				s2 := pubsub.NewStreamFromDescription[float64](pubsub.MakeStreamDescription[float64]("same", true, false))
				defer pubsub.ForceRemoveStream(s1.Description())
				defer pubsub.ForceRemoveStream(s2.Description())

				pubsub.AddOrReplaceStream(s1)
				pubsub.AddOrReplaceStream(s2)

				r1, err1 := pubsub.GetStream(s1.ID())
				r2, err2 := pubsub.GetStream(s2.ID())

				Expect(err1).To(BeNil())
				Expect(err2).To(BeNil())
				Expect(r1.ID().Topic).To(Equal(r2.ID().Topic))
				Expect(r1.ID().TopicType).ToNot(Equal(r2.ID().TopicType))

			})
			It("supports replacing streams", func() {
				topic := "streamA"
				s1 := pubsub.NewStreamFromDescription[int](pubsub.MakeStreamDescription[int](topic, false, false))
				s2 := pubsub.NewStreamFromDescription[int](pubsub.MakeStreamDescription[int](topic, true, false))
				defer pubsub.ForceRemoveStream(s1.Description())
				defer pubsub.ForceRemoveStream(s2.Description())

				pubsub.AddOrReplaceStream(s1)
				pubsub.AddOrReplaceStream(s2)

				r1, err1 := pubsub.GetStream(s1.ID())
				_, err2 := pubsub.GetStream(s2.ID())

				Expect(err1).To(BeNil())
				Expect(err2).To(BeNil())
				Expect(r1.Description().AsyncStream).To(BeTrue())
			})
		})
		Context("a stream", func() {
			It("sends and receives event via the pub sub system", func() {
				var topic = "test-send-rec-1"
				s := pubsub.NewStreamFromDescription[string](pubsub.MakeStreamDescription[string](topic, false, false))
				s.Run()
				defer pubsub.ForceRemoveStream(s.Description())

				id := s.ID()

				pubsub.AddOrReplaceStream(s)
				rec, _ := pubsub.SubscribeByTopicID[string](id)

				e1 := events.NewEvent("test 1")
				go func() {
					publisher, _ := pubsub.RegisterPublisher[string](s.ID())
					publisher.Publish(e1)
				}()
				eResult := <-rec.Notify()

				Expect(e1).To(Equal(eResult))
			})
		})

		Describe("GetOrAddStream", func() {
			It("adds a new stream if it doesn't exist", func() {
				topic := "get-or-add-1"
				desc := pubsub.MakeStreamDescription[int](topic, false, false)
				s, err := pubsub.GetOrAddStream[int](desc)
				Expect(err).To(BeNil())
				Expect(s).NotTo(BeNil())
				Expect(s.ID().Topic).To(Equal(topic))
				pubsub.ForceRemoveStream(desc)
			})

			It("returns existing stream if it exists", func() {
				topic := "get-or-add-2"
				desc := pubsub.MakeStreamDescription[int](topic, false, false)
				s1, _ := pubsub.GetOrAddStream[int](desc)
				s2, err := pubsub.GetOrAddStream[int](desc)
				Expect(err).To(BeNil())
				Expect(s1).To(Equal(s2))
				pubsub.ForceRemoveStream(desc)
			})
		})

		Describe("InstantPublishByTopic", func() {
			It("fails if stream does not exist", func() {
				err := pubsub.InstantPublishByTopic("non-existent-topic", events.NewEvent("hello"))
				Expect(err).NotTo(BeNil())
				Expect(err).To(Equal(pubsub.StreamNotFoundError))
			})
		})

		Describe("UnRegisterPublisher", func() {
			It("handles nil publisher gracefully", func() {
				err := pubsub.UnRegisterPublisher[int](nil)
				Expect(err).To(BeNil())
			})
		})

		Describe("SubscribeByTopic", func() {
			It("subscribes to an existing stream", func() {
				topic := "subscribe-by-topic-1"
				desc := pubsub.MakeStreamDescription[int](topic, false, false)
				_, err := pubsub.AddOrReplaceStreamFromDescription[int](desc)
				Expect(err).To(BeNil())
				defer pubsub.ForceRemoveStream(desc)

				rec, err := pubsub.SubscribeByTopic[int](topic)
				Expect(err).To(BeNil())
				Expect(rec).NotTo(BeNil())
				Expect(rec.StreamID().Topic).To(Equal(topic))
			})
			It("fails if stream does not exist", func() {
				rec, err := pubsub.SubscribeByTopic[int]("non-existent-topic-sub")
				Expect(err).To(Equal(pubsub.StreamNotFoundError))
				Expect(rec).To(BeNil())
			})
		})
	})
})
