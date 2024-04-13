package pubsub_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/pkg/events"
	pubsub2 "go-stream-processing/pkg/pubsub"
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
				d, _ := pubsub2.StreamDescriptionFromYML([]byte(yml))
				pubsub2.AddOrReplaceStreamD[map[string]interface{}](d)

				r, err := pubsub2.GetStream[map[string]interface{}](d.ID)
				Expect(err).To(BeNil())
				Expect(r.Description()).To(Equal(d))
			})
			It("not retrieves the stream when it does not exist", func() {
				_, err := pubsub2.GetStream[int]("not-existing-name")
				Expect(err).To(Equal(pubsub2.StreamNotFoundError()))
			})
			It("not retrieves the stream when a type mismatches", func() {
				var yml = `
name: test-retrieve-2
id: 3c191d62-6575-4951-a9e7-4ec83c947251
async: true
`
				d, _ := pubsub2.StreamDescriptionFromYML([]byte(yml))
				pubsub2.AddOrReplaceStreamD[map[string]interface{}](d)

				_, err := pubsub2.GetStream[int](d.ID)
				Expect(err).To(Equal(pubsub2.StreamTypeMismatchError()))
			})
		})

		Describe("Create with Description", func() {
			It("registers a Stream", func() {
				var yml = `
name: test.pub.sub
id: 3c191d62-6574-4951-a9e6-4ec83c947250
async: true
`
				d, _ := pubsub2.StreamDescriptionFromYML([]byte(yml))
				pubsub2.AddOrReplaceStreamD[map[string]interface{}](d)

				_, err := pubsub2.GetStream[map[string]interface{}](d.StreamID())
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
				d, _ := pubsub2.StreamDescriptionFromYML([]byte(yml))
				s, err := pubsub2.AddOrReplaceStreamD[map[string]interface{}](d)
				s.Run()

				pubsub2.RemoveStreamD[map[string]interface{}](d)

				_, err = pubsub2.GetStream[map[string]interface{}](d.StreamID())
				Expect(err).To(Equal(pubsub2.StreamNotFoundError()))

			})
		})
		Context("adding new pubsub", func() {
			It("is successful when the stream does not yet exists", func() {
				var id pubsub2.StreamID = "test-ps-1"
				s := pubsub2.NewLocalSyncStream[string](pubsub2.NewStreamDescription(id, false))

				_ = pubsub2.AddOrReplaceStream[string](s)

				r, e := pubsub2.GetStream[string](id)
				Expect(r).To(Equal(s))
				Expect(e).To(BeNil())
			})
			It("is NOT successful when the stream id is invalid", func() {

				s1 := pubsub2.NewLocalSyncStream[map[string]interface{}](pubsub2.NewStreamDescription("", false))

				err := pubsub2.AddOrReplaceStream[map[string]interface{}](s1)

				Expect(err).To(Equal(pubsub2.StreamIDNilError()))
			})
		})
		Context("getting stream in pubsub system", func() {
			It("results in an error if not existing", func() {
				id := pubsub2.StreamID(uuid.New().String())
				_, e := pubsub2.GetStream[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("subscribing to a non existing stream", func() {
			It("ends up in an error", func() {
				id := pubsub2.StreamID(uuid.New().String())
				_, e := pubsub2.Subscribe[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("unsub from a stream", func() {
			It("is successful when the stream exists", func() {
				var id pubsub2.StreamID = "test-unsub-1"
				s := pubsub2.NewLocalSyncStream[string](pubsub2.NewStreamDescription(id, false))
				pubsub2.AddOrReplaceStream[string](s)

				rec, _ := pubsub2.Subscribe[string](id)

				Expect(func() { pubsub2.Unsubscribe[string](rec) }).NotTo(Panic())
			})
		})
		Context("unsub from non existing stream", func() {
			It("ends up in no error", func() {
				rec := &pubsub2.StreamReceiver[string]{
					StreamID: pubsub2.StreamID(uuid.New().String()),
					ID:       pubsub2.StreamReceiverID(uuid.New()),
					Notify:   make(chan events.Event[string]),
				}

				Expect(func() { pubsub2.Unsubscribe[string](rec) }).NotTo(Panic())
			})
		})
		Context("a stream", func() {
			It("sends and receives event via the pub sub system", func() {
				var id pubsub2.StreamID = "test-send-rec-1"
				s := pubsub2.NewLocalSyncStream[string](pubsub2.NewStreamDescription("test-send-rec-1", false))
				s.Run()
				defer s.TryClose()

				pubsub2.AddOrReplaceStream[string](s)
				rec, _ := pubsub2.Subscribe[string](id)

				e1 := events.NewEvent("test 1")
				go func() {
					pubsub2.Publish(pubsub2.StreamID(id), e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
		})
	})
})
