package pubsub_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/internal/events"
	"go-stream-processing/internal/pubsub"
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

				r, err := pubsub.GetStreamN[map[string]interface{}](d.Name)
				Expect(err).To(BeNil())
				Expect(r.Description()).To(Equal(d))
			})
			It("not retrieves the stream when it does not exist", func() {
				_, err := pubsub.GetStreamN[int]("not-existing-name")
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

				_, err := pubsub.GetStreamN[int](d.Name)
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

				pubsub.RemoveStreamD[map[string]interface{}](d)

				_, err = pubsub.GetStream[map[string]interface{}](d.StreamID())
				Expect(err).To(Equal(pubsub.StreamNotFoundError()))

			})
		})
		Context("adding new pubsub", func() {
			It("is successful when the stream does not yet exists", func() {
				id := pubsub.StreamID(uuid.New())
				s := pubsub.NewLocalSyncStream[string](pubsub.NewStreamDescription("test-ps-1", uuid.UUID(id), false))

				_ = pubsub.AddOrReplaceStream[string](s)

				r, e := pubsub.GetStream[string](id)
				Expect(r).To(Equal(s))
				Expect(e).To(BeNil())
			})
			It("is NOT successful when the stream name is taken", func() {
				id := pubsub.StreamID(uuid.New())
				s1 := pubsub.NewLocalSyncStream[string](pubsub.NewStreamDescription("test-ps-2", uuid.UUID(id), false))
				s2 := pubsub.NewLocalSyncStream[string](pubsub.NewStreamDescription("test-ps-2", uuid.New(), false))

				pubsub.AddOrReplaceStream[string](s1)
				err := pubsub.AddOrReplaceStream[string](s2)

				Expect(err).To(Equal(pubsub.StreamNameExistsError()))
			})
			It("is NOT successful when the stream id is invalid", func() {
				id := uuid.Nil
				s1 := pubsub.NewLocalSyncStream[map[string]interface{}](pubsub.NewStreamDescription("test-ps-3", id, false))

				err := pubsub.AddOrReplaceStream[map[string]interface{}](s1)

				Expect(err).To(Equal(pubsub.StreamIDNilError()))
			})
			It("is NOT successful when the stream name and id deviate in the header", func() {
				id := uuid.New()
				s1 := pubsub.NewLocalSyncStream[string](pubsub.NewStreamDescription("test-ps-4", id, false))
				s2 := pubsub.NewLocalSyncStream[string](pubsub.NewStreamDescription("test-ps-5", id, false))

				pubsub.AddOrReplaceStream[string](s1)
				err := pubsub.AddOrReplaceStream[string](s2)

				Expect(err).To(Equal(pubsub.StreamIDNameDivError()))
			})
		})
		Context("getting pubsub", func() {
			It("results in an error if not existing", func() {
				id := pubsub.StreamID(uuid.New())
				_, e := pubsub.GetStream[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("subscribing to a non existing stream", func() {
			It("ends up in an error", func() {
				id := pubsub.StreamID(uuid.New())
				_, e := pubsub.Subscribe[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("unsub from a stream", func() {
			It("is successful when the stream exists", func() {
				id := uuid.New()
				s := pubsub.NewLocalSyncStream[string](pubsub.NewStreamDescription("test-unsub-1", id, false))
				pubsub.AddOrReplaceStream[string](s)

				rec, _ := pubsub.Subscribe[string](pubsub.StreamID(id))

				Expect(func() { pubsub.Unsubscribe[string](rec) }).NotTo(Panic())
			})
		})
		Context("unsub from non existing stream", func() {
			It("ends up in no error", func() {
				rec := &pubsub.StreamReceiver[string]{
					StreamID: pubsub.StreamID(uuid.New()),
					ID:       pubsub.StreamReceiverID(uuid.New()),
					Notify:   make(chan events.Event[string]),
				}

				Expect(func() { pubsub.Unsubscribe[string](rec) }).NotTo(Panic())
			})
		})
		Context("a stream", func() {
			It("sends and receives event via the pub sub system", func() {
				id := uuid.New()
				s := pubsub.NewLocalSyncStream[string](pubsub.NewStreamDescription("test-send-rec-1", id, false))
				s.Run()
				defer s.TryClose()

				pubsub.AddOrReplaceStream[string](s)
				rec, _ := pubsub.Subscribe[string](pubsub.StreamID(id))

				e1 := events.NewEvent("test 1")
				go func() {
					pubsub.Publish(pubsub.StreamID(id), e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
			It("sends and receives event via the pub sub system by name", func() {
				id := uuid.New()
				name := "test-send-rec-2"
				s := pubsub.NewLocalAsyncStream[string](pubsub.NewStreamDescription(name, id, false))
				s.Run()
				defer s.TryClose()

				pubsub.AddOrReplaceStream[string](s)
				rec, _ := pubsub.Subscribe[string](pubsub.StreamID(id))

				e1 := events.NewEvent("test 1")
				go func() {
					pubsub.PublishN(name, e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
		})
	})
})
