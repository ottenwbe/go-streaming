package streams_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/internal/events"
	streams2 "go-stream-processing/internal/streams"
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
				d, _ := streams2.StreamDescriptionFromYML([]byte(yml))
				streams2.NewOrReplaceStreamD[map[string]interface{}](d)

				r, err := streams2.GetStreamN[map[string]interface{}](d.Name)
				Expect(err).To(BeNil())
				Expect(r.Description()).To(Equal(d))
			})
			It("not retrieves the stream when it does not exist", func() {
				_, err := streams2.GetStreamN[int]("not-existing-name")
				Expect(err).To(Equal(streams2.StreamNotFoundError()))
			})
			It("not retrieves the stream when a type mismatches", func() {
				var yml = `
name: test-retrieve-2
id: 3c191d62-6575-4951-a9e7-4ec83c947251
async: true
`
				d, _ := streams2.StreamDescriptionFromYML([]byte(yml))
				streams2.NewOrReplaceStreamD[map[string]interface{}](d)

				_, err := streams2.GetStreamN[int](d.Name)
				Expect(err).To(Equal(streams2.StreamTypeMismatchError()))
			})
		})

		Describe("Create with Description", func() {
			It("registers a Stream", func() {
				var yml = `
name: test
id: 3c191d62-6574-4951-a9e6-4ec83c947250
async: true
`
				d, _ := streams2.StreamDescriptionFromYML([]byte(yml))
				streams2.NewOrReplaceStreamD[map[string]interface{}](d)

				_, err := streams2.GetStream[map[string]interface{}](d.StreamID())
				Expect(err).To(BeNil())

			})
		})
		Describe("Delete", func() {
			It("removes streams", func() {
				var yml = `
name: test-delete
id: 4c191d62-6574-4951-a9e6-4ec83c947250
async: true
`
				d, _ := streams2.StreamDescriptionFromYML([]byte(yml))

				streams2.NewOrReplaceStreamD[map[string]interface{}](d)
				streams2.RemoveStreamD[map[string]interface{}](d)

				_, err := streams2.GetStream[map[string]interface{}](d.StreamID())
				Expect(err).To(Equal(streams2.StreamNotFoundError()))

			})
		})
		Context("adding new streams", func() {
			It("is successful when the stream does not yet exists", func() {
				id := streams2.StreamID(uuid.New())
				s := streams2.NewLocalSyncStream[string](streams2.NewStreamDescription("test-ps-1", uuid.UUID(id), false))

				_ = streams2.NewOrReplaceStream[string](s)

				r, e := streams2.GetStream[string](id)
				Expect(r).To(Equal(s))
				Expect(e).To(BeNil())
			})
			It("is NOT successful when the stream name is taken", func() {
				id := streams2.StreamID(uuid.New())
				s1 := streams2.NewLocalSyncStream[string](streams2.NewStreamDescription("test-ps-2", uuid.UUID(id), false))
				s2 := streams2.NewLocalSyncStream[string](streams2.NewStreamDescription("test-ps-2", uuid.New(), false))

				streams2.NewOrReplaceStream[string](s1)
				err := streams2.NewOrReplaceStream[string](s2)

				Expect(err).To(Equal(streams2.StreamNameExistsError()))
			})
			It("is NOT successful when the stream id is invalid", func() {
				id := uuid.Nil
				s1 := streams2.NewLocalSyncStream[map[string]interface{}](streams2.NewStreamDescription("test-ps-3", id, false))

				err := streams2.NewOrReplaceStream[map[string]interface{}](s1)

				Expect(err).To(Equal(streams2.StreamIDNilError()))
			})
			It("is NOT successful when the stream name and id deviate in the header", func() {
				id := uuid.New()
				s1 := streams2.NewLocalSyncStream[string](streams2.NewStreamDescription("test-ps-4", id, false))
				s2 := streams2.NewLocalSyncStream[string](streams2.NewStreamDescription("test-ps-5", id, false))

				streams2.NewOrReplaceStream[string](s1)
				err := streams2.NewOrReplaceStream[string](s2)

				Expect(err).To(Equal(streams2.StreamIDNameDivError()))
			})
		})
		Context("getting streams", func() {
			It("results in an error if not existing", func() {
				id := streams2.StreamID(uuid.New())
				_, e := streams2.GetStream[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("subscribing to a non existing stream", func() {
			It("ends up in an error", func() {
				id := streams2.StreamID(uuid.New())
				_, e := streams2.Subscribe[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("unsub from a stream", func() {
			It("is successful when the stream exists", func() {
				id := uuid.New()
				s := streams2.NewLocalSyncStream[string](streams2.NewStreamDescription("test-unsub-1", id, false))
				streams2.NewOrReplaceStream[string](s)

				rec, _ := streams2.Subscribe[string](streams2.StreamID(id))
				e := streams2.Unsubscribe(rec)

				Expect(e).To(BeNil())
			})
		})
		Context("unsub from non existing stream", func() {
			It("ends up in an error", func() {
				rec := &streams2.StreamReceiver[string]{
					StreamID: streams2.StreamID(uuid.New()),
					ID:       streams2.StreamReceiverID(uuid.New()),
					Notify:   make(chan events.Event[string]),
				}
				e := streams2.Unsubscribe[string](rec)
				Expect(e).NotTo(BeNil())
				Expect(e).To(Equal(streams2.StreamNotFoundError()))
			})
		})
		Context("a stream", func() {
			It("sends and receives event via the pub sub system", func() {
				id := uuid.New()
				s := streams2.NewLocalSyncStream[string](streams2.NewStreamDescription("test-send-rec-1", id, false))
				s.Start()
				defer s.Stop()

				streams2.NewOrReplaceStream[string](s)
				rec, _ := streams2.Subscribe[string](streams2.StreamID(id))

				e1 := events.NewEvent("test 1")
				go func() {
					streams2.Publish(streams2.StreamID(id), e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
			It("sends and receives event via the pub sub system by name", func() {
				id := uuid.New()
				name := "test-send-rec-2"
				s := streams2.NewLocalAsyncStream[string](streams2.NewStreamDescription(name, id, false))
				s.Start()
				defer s.Stop()

				streams2.NewOrReplaceStream[string](s)
				rec, _ := streams2.Subscribe[string](streams2.StreamID(id))

				e1 := events.NewEvent("test 1")
				go func() {
					streams2.PublishN(name, e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
		})
	})
})
