package pubsub_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
)

var _ = Describe("PubSub", func() {
	Describe("PubSub System", func() {

		Describe("Get Stream by Name", func() {
			It("retrieves the stream", func() {
				var yml = `
id: 
  topic: 3c191d62-6575-4951-a9e6-4ec83c947251
  type: map[string]interface {}
async: true
`
				d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
				pubsub.AddOrReplaceStreamD[map[string]interface{}](d)

				r, err := pubsub.GetStream[map[string]interface{}](d.ID)
				Expect(err).To(BeNil())
				Expect(r.Description()).To(Equal(d))
			})
			It("not retrieves the stream when it does not exist", func() {
				_, err := pubsub.GetStreamByTopic[int]("not-existing-name")
				Expect(err).To(Equal(pubsub.StreamNotFoundError()))
			})
			It("not retrieves the stream when a type mismatches", func() {
				var yml = `
id: 
  topic: 3c191d62-6575-4951-a9e7-4ec83c947251
  type: map[string]interface {}
async: true
`
				d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
				pubsub.AddOrReplaceStreamD[map[string]interface{}](d)

				_, err := pubsub.GetStream[int](d.ID)
				Expect(err).To(Equal(pubsub.StreamTypeMismatchError()))
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
				pubsub.AddOrReplaceStreamD[map[string]interface{}](d)

				_, err := pubsub.GetStream[map[string]interface{}](d.StreamID())
				Expect(err).To(BeNil())

			})
		})
		Describe("Delete", func() {
			It("removes pubsub", func() {
				var yml = `
id: 
  topic: 4c191d62-6574-4951-a9e6-4ec83c947250
  type: map[string]interface{}
async: true
`
				d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
				s, err := pubsub.AddOrReplaceStreamD[map[string]interface{}](d)
				s.Run()

				pubsub.ForceRemoveStreamD(d)

				_, err = pubsub.GetStream[map[string]interface{}](d.StreamID())
				Expect(err).To(Equal(pubsub.StreamNotFoundError()))

			})
		})
		Context("adding new pubsub", func() {
			It("is successful when the stream does not yet exists", func() {
				var topic = "test-ps-1"
				s := pubsub.NewLocalSyncStream[string](pubsub.MakeStreamDescription[string](topic, false))

				_ = pubsub.AddOrReplaceStream[string](s)

				r, e := pubsub.GetStreamByTopic[string](topic)
				Expect(r).To(Equal(s))
				Expect(e).To(BeNil())
			})
			It("is NOT successful when the stream id is invalid", func() {

				s1 := pubsub.NewLocalSyncStream[map[string]interface{}](pubsub.MakeStreamDescriptionID(pubsub.NilStreamID(), false))
				err := pubsub.AddOrReplaceStream[map[string]interface{}](s1)

				Expect(err).To(Equal(pubsub.StreamIDNilError()))
			})
		})
		Context("getting stream in pubsub system", func() {
			It("results in an error if non-existing", func() {
				id := pubsub.RandomStreamID()
				_, e := pubsub.GetStream[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("subscribing to a non existing stream", func() {
			It("ends up in an error", func() {
				id := pubsub.RandomStreamID()
				_, e := pubsub.Subscribe[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("unsub from a stream", func() {
			It("is successful when the stream exists", func() {
				var topic = "test-unsub-1"
				s := pubsub.NewLocalSyncStream[string](pubsub.MakeStreamDescription[string](topic, false))
				pubsub.AddOrReplaceStream[string](s)

				rec, _ := pubsub.Subscribe[string](s.ID())

				Expect(func() { pubsub.Unsubscribe[string](rec) }).NotTo(Panic())
			})
		})
		Context("unsub from non existing stream", func() {
			It("ends up in no error", func() {
				rec := &pubsub.StreamReceiver[string]{
					StreamID: pubsub.RandomStreamID(),
					ID:       pubsub.StreamReceiverID(uuid.New()),
					Notify:   make(chan events.Event[string]),
				}

				Expect(func() { pubsub.Unsubscribe[string](rec) }).NotTo(Panic())
			})
		})
		Context("streams with same name and different types", func() {
			It("can exist", func() {
				s1 := pubsub.NewLocalSyncStream[int](pubsub.MakeStreamDescription[int]("same", false))
				s2 := pubsub.NewLocalSyncStream[float64](pubsub.MakeStreamDescription[float64]("same", true))

				pubsub.AddOrReplaceStream[int](s1)
				pubsub.AddOrReplaceStream[float64](s2)

				r1, err1 := pubsub.GetStream[int](s1.ID())
				r2, err2 := pubsub.GetStream[float64](s2.ID())

				Expect(err1).To(BeNil())
				Expect(err2).To(BeNil())
				Expect(r1.ID().Topic).To(Equal(r2.ID().Topic))
				Expect(r1.ID().TopicType).ToNot(Equal(r2.ID().TopicType))

			})
		})
		Context("a stream", func() {
			It("sends and receives event via the pub sub system", func() {
				var topic = "test-send-rec-1"
				s := pubsub.NewLocalSyncStream[string](pubsub.MakeStreamDescription[string](topic, false))
				s.Run()
				defer s.TryClose()

				id := s.ID()

				pubsub.AddOrReplaceStream[string](s)
				rec, _ := pubsub.Subscribe[string](id)

				e1 := events.NewEvent("test 1")
				go func() {
					pubsub.Publish(pubsub.StreamID(id), e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
		})
	})
})
