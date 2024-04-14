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
name: test-retrieve
id: 3c191d62-6575-4951-a9e6-4ec83c947251
async: true
`
				d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
				pubsub.AddOrReplaceStreamD[map[string]interface{}](d)

				r, err := pubsub.GetStream[map[string]interface{}](d.ID)
				Expect(err).To(BeNil())
				Expect(r.Description()).To(Equal(d))
			})
			It("not retrieves the stream when it does not exist", func() {
				_, err := pubsub.GetStream[int]("not-existing-name")
				Expect(err).To(Equal(pubsub.StreamNotFoundError()))
			})
			It("not retrieves the stream when a type mismatches", func() {
				var yml = `
name: test-retrieve-2
id: 3c191d62-6575-4951-a9e7-4ec83c947251
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
name: test.pub.sub
id: 3c191d62-6574-4951-a9e6-4ec83c947250
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
name: test-delete-ps
id: 4c191d62-6574-4951-a9e6-4ec83c947250
async: true
`
				d, _ := pubsub.StreamDescriptionFromYML([]byte(yml))
				s, err := pubsub.AddOrReplaceStreamD[map[string]interface{}](d)
				s.Run()

				pubsub.ForceRemoveStreamD[map[string]interface{}](d)

				_, err = pubsub.GetStream[map[string]interface{}](d.StreamID())
				Expect(err).To(Equal(pubsub.StreamNotFoundError()))

			})
		})
		Context("adding new pubsub", func() {
			It("is successful when the stream does not yet exists", func() {
				var id pubsub.StreamID = "test-ps-1"
				s := pubsub.NewLocalSyncStream[string](pubsub.MakeStreamDescription(id, false))

				_ = pubsub.AddOrReplaceStream[string](s)

				r, e := pubsub.GetStream[string](id)
				Expect(r).To(Equal(s))
				Expect(e).To(BeNil())
			})
			It("is NOT successful when the stream id is invalid", func() {

				s1 := pubsub.NewLocalSyncStream[map[string]interface{}](pubsub.MakeStreamDescription("", false))

				err := pubsub.AddOrReplaceStream[map[string]interface{}](s1)

				Expect(err).To(Equal(pubsub.StreamIDNilError()))
			})
		})
		Context("getting stream in pubsub system", func() {
			It("results in an error if not existing", func() {
				id := pubsub.StreamID(uuid.New().String())
				_, e := pubsub.GetStream[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("subscribing to a non existing stream", func() {
			It("ends up in an error", func() {
				id := pubsub.StreamID(uuid.New().String())
				_, e := pubsub.Subscribe[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("unsub from a stream", func() {
			It("is successful when the stream exists", func() {
				var id pubsub.StreamID = "test-unsub-1"
				s := pubsub.NewLocalSyncStream[string](pubsub.MakeStreamDescription(id, false))
				pubsub.AddOrReplaceStream[string](s)

				rec, _ := pubsub.Subscribe[string](id)

				Expect(func() { pubsub.Unsubscribe[string](rec) }).NotTo(Panic())
			})
		})
		Context("unsub from non existing stream", func() {
			It("ends up in no error", func() {
				rec := &pubsub.StreamReceiver[string]{
					StreamID: pubsub.StreamID(uuid.New().String()),
					ID:       pubsub.StreamReceiverID(uuid.New()),
					Notify:   make(chan events.Event[string]),
				}

				Expect(func() { pubsub.Unsubscribe[string](rec) }).NotTo(Panic())
			})
		})
		Context("a stream", func() {
			It("sends and receives event via the pub sub system", func() {
				var id pubsub.StreamID = "test-send-rec-1"
				s := pubsub.NewLocalSyncStream[string](pubsub.MakeStreamDescription("test-send-rec-1", false))
				s.Run()
				defer s.TryClose()

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
