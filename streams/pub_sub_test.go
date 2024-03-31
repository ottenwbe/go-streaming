package streams_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/events"
	"go-stream-processing/streams"
)

var _ = Describe("PubSub", func() {
	Describe("PubSub System", func() {
		Describe("Create with Description", func() {
			It("registers a Stream", func() {
				var yml = `
name: test
id: 3c191d62-6574-4951-a9e6-4ec83c947250
async: true
`
				d, _ := streams.StreamDescriptionFromYML([]byte(yml))
				streams.NewOrReplaceStreamD[map[string]interface{}](d)

				_, err := streams.GetStream[map[string]interface{}](d.StreamID())
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
				d, _ := streams.StreamDescriptionFromYML([]byte(yml))

				streams.NewOrReplaceStreamD[map[string]interface{}](d)
				streams.RemoveStreamD[map[string]interface{}](d)

				_, err := streams.GetStream[map[string]interface{}](d.StreamID())
				Expect(err).To(Equal(streams.StreamNotFoundError()))

			})
		})
		Context("adding new streams", func() {
			It("is successful when the stream does not yet exists", func() {
				id := streams.StreamID(uuid.New())
				s := streams.NewLocalSyncStream[string](streams.NewStreamDescription("test-ps-1", uuid.UUID(id), false))

				_ = streams.NewOrReplaceStream[string](s)

				r, e := streams.GetStream[string](id)
				Expect(r).To(Equal(s))
				Expect(e).To(BeNil())
			})
			It("is NOT successful when the stream name is taken", func() {
				id := streams.StreamID(uuid.New())
				s1 := streams.NewLocalSyncStream[string](streams.NewStreamDescription("test-ps-2", uuid.UUID(id), false))
				s2 := streams.NewLocalSyncStream[string](streams.NewStreamDescription("test-ps-2", uuid.New(), false))

				streams.NewOrReplaceStream[string](s1)
				err := streams.NewOrReplaceStream[string](s2)

				Expect(err).To(Equal(streams.StreamNameExistsError()))
			})
			It("is NOT successful when the stream id is invalid", func() {
				id := uuid.Nil
				s1 := streams.NewLocalSyncStream[map[string]interface{}](streams.NewStreamDescription("test-ps-3", id, false))

				err := streams.NewOrReplaceStream[map[string]interface{}](s1)

				Expect(err).To(Equal(streams.StreamIDNilError()))
			})
			It("is NOT successful when the stream name and id deviate in the header", func() {
				id := uuid.New()
				s1 := streams.NewLocalSyncStream[string](streams.NewStreamDescription("test-ps-4", id, false))
				s2 := streams.NewLocalSyncStream[string](streams.NewStreamDescription("test-ps-5", id, false))

				streams.NewOrReplaceStream[string](s1)
				err := streams.NewOrReplaceStream[string](s2)

				Expect(err).To(Equal(streams.StreamIDNameDivError()))
			})
		})
		Context("getting streams", func() {
			It("results in an error if not existing", func() {
				id := streams.StreamID(uuid.New())
				_, e := streams.GetStream[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("subscribing to a non existing stream", func() {
			It("ends up in an error", func() {
				id := streams.StreamID(uuid.New())
				_, e := streams.Subscribe[int](id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("unsub from a stream", func() {
			It("is successful when the stream exists", func() {
				id := uuid.New()
				s := streams.NewLocalSyncStream[string](streams.NewStreamDescription("test-unsub-1", id, false))
				streams.NewOrReplaceStream[string](s)

				rec, _ := streams.Subscribe[string](streams.StreamID(id))
				e := streams.Unsubscribe(rec)

				Expect(e).To(BeNil())
			})
		})
		Context("unsub from non existing stream", func() {
			It("ends up in an error", func() {
				rec := &streams.StreamReceiver[string]{
					StreamID: streams.StreamID(uuid.New()),
					ID:       streams.StreamReceiverID(uuid.New()),
					Notify:   make(chan events.Event[string]),
				}
				e := streams.Unsubscribe[string](rec)
				Expect(e).NotTo(BeNil())
				Expect(e).To(Equal(streams.StreamNotFoundError()))
			})
		})
		Context("a stream", func() {
			It("sends and receives event via the pub sub system", func() {
				id := uuid.New()
				s := streams.NewLocalSyncStream[string](streams.NewStreamDescription("test-send-rec-1", id, false))
				s.Start()
				defer s.Stop()

				streams.NewOrReplaceStream[string](s)
				rec, _ := streams.Subscribe[string](streams.StreamID(id))

				e1 := events.NewEvent("test 1")
				go func() {
					streams.Publish(streams.StreamID(id), e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
			It("sends and receives event via the pub sub system by name", func() {
				id := uuid.New()
				name := "test-send-rec-2"
				s := streams.NewLocalAsyncStream[string](streams.NewStreamDescription(name, id, false))
				s.Start()
				defer s.Stop()

				streams.NewOrReplaceStream[string](s)
				rec, _ := streams.Subscribe[string](streams.StreamID(id))

				e1 := events.NewEvent("test 1")
				go func() {
					streams.PublishN(name, e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
		})
	})
})
