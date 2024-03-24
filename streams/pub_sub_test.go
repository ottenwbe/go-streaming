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
		Context("adding streamIndex", func() {
			It("finds all added streamIndex", func() {
				id := streams.StreamID(uuid.New())
				s := streams.NewLocalSyncStream("test", id)
				streams.PubSubSystem.NewOrReplaceStream(id, s)

				r, e := streams.PubSubSystem.Get(id)
				Expect(r).To(Equal(s))
				Expect(e).To(BeNil())
			})
		})
		Context("getting streamIndex", func() {
			It("results in an error if not existing", func() {
				id := streams.StreamID(uuid.New())
				_, e := streams.PubSubSystem.Get(id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("subscribing to a non existing stream", func() {
			It("ends up in an error", func() {
				id := streams.StreamID(uuid.New())
				_, e := streams.PubSubSystem.Subscribe(id)
				Expect(e).NotTo(BeNil())
			})
		})
		Context("unsub from a stream", func() {
			It("is successful when the stream exists", func() {
				id := streams.StreamID(uuid.New())
				s := streams.NewLocalSyncStream("test", id)
				streams.PubSubSystem.NewOrReplaceStream(id, s)

				rec, _ := streams.PubSubSystem.Subscribe(id)
				e := streams.PubSubSystem.Unsubscribe(rec)

				Expect(e).To(BeNil())
			})
		})
		Context("unsub from non existing stream", func() {
			It("ends up in an error", func() {
				rec := &streams.StreamReceiver{
					StreamID: streams.StreamID(uuid.New()),
					ID:       streams.StreamReceiverID(uuid.New()),
					Notify:   make(chan events.Event),
				}
				e := streams.PubSubSystem.Unsubscribe(rec)
				Expect(e).NotTo(BeNil())
				Expect(e).To(Equal(streams.StreamNotFoundError()))
			})
		})
		Context("a stream", func() {
			It("sends and receives event via the pub sub system", func() {
				id := streams.StreamID(uuid.New())
				s := streams.NewLocalSyncStream("test", id)
				s.Start()
				defer s.Stop()

				streams.PubSubSystem.NewOrReplaceStream(id, s)
				rec, _ := streams.PubSubSystem.Subscribe(id)

				e1, _ := events.NewEvent("test 1")
				go func() {
					streams.PubSubSystem.Publish(id, e1)
				}()
				eResult := <-rec.Notify

				Expect(e1).To(Equal(eResult))
			})
		})
	})
})
