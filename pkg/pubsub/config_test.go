package pubsub_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var _ = Describe("Descriptions", func() {

	BeforeEach(func() {
		pubsub.RegisterType[int]()
		pubsub.RegisterType[string]()
		pubsub.RegisterType[float64]()
		pubsub.RegisterType[float32]()
	})

	AfterEach(func() {
		pubsub.UnRegisterType[int]()
		pubsub.UnRegisterType[string]()
		pubsub.UnRegisterType[float64]()
		pubsub.UnRegisterType[float32]()
	})

	Describe("MakeSubscriberConfig", func() {
		It("creates a configuration with defaults", func() {
			d := pubsub.MakeSubscriberConfig()
			Expect(d.Synchronous).To(BeFalse())
			Expect(d.BufferCapacity).To(Equal(0))
			Expect(d.BufferPolicySelection).To(Equal(events.SelectionPolicyConfig{}))
		})
	})

	Describe("MakeStreamConfig", func() {
		It("creates a configuration with defaults", func() {
			d := pubsub.MakeStreamConfig[int]("topic")
			Expect(d.ID.Topic).To(Equal("topic"))
			Expect(d.Asynchronous).To(BeFalse())
			Expect(d.AutoCleanup).To(BeFalse())
			Expect(d.BufferCapacity).To(Equal(0))
			Expect(d.DefaultSubscribers).To(Equal(pubsub.MakeSubscriberConfig()))
			Expect(d.Sort).To(BeFalse())
		})

		It("applies options correctly", func() {
			v := pubsub.MakeSubscriberConfig()

			d := pubsub.MakeStreamConfig[int]("topic",
				pubsub.WithAsynchronousStream(true),
				pubsub.WithAutoCleanup(true),
				pubsub.WithDefaultSubscribers(v),
				pubsub.WithSorted(true),
			)
			Expect(d.Asynchronous).To(BeTrue())
			Expect(d.AutoCleanup).To(BeTrue())
			Expect(d.DefaultSubscribers).To(Equal(v))
			Expect(d.Sort).To(BeTrue())
		})
	})

	Describe("MakeStreamConfigByID", func() {
		It("creates a configuration from ID with options", func() {
			id := pubsub.MakeStreamID[string]("topic-id")
			d := pubsub.MakeStreamConfigByID(id, pubsub.WithAsynchronousStream(true))
			Expect(d.ID).To(Equal(id))
			Expect(d.Asynchronous).To(BeTrue())
		})
	})

	Describe("stream Config", func() {
		Context("Parsing YAML", func() {
			It("can be parsed correctly with all fields", func() {

				var yml = `
---
id: 
  topic: 3c191d62-6574-4951-a8e6-4ec83c947250
  type: string
asyncStream: true
autoCleanup: true
sorted: true
`
				v, err := pubsub.StreamDescriptionFromYML([]byte(yml))
				Expect(err).To(BeNil())
				Expect(v.ID).To(Equal(pubsub.MakeStreamID[string]("3c191d62-6574-4951-a8e6-4ec83c947250")))
				Expect(v.Asynchronous).To(BeTrue())
				Expect(v.AutoCleanup).To(BeTrue())
				Expect(v.Sort).To(BeTrue())
			})

			It("parses correctly with default values", func() {
				var yml = `
id: 
  topic: test-defaults
  type: int
`
				v, err := pubsub.StreamDescriptionFromYML([]byte(yml))
				Expect(err).To(BeNil())
				Expect(v.Asynchronous).To(BeFalse())
				Expect(v.AutoCleanup).To(BeFalse())
			})
		})

		Context("Parsing JSON", func() {
			It("can be parsed correctly", func() {
				var jsonStr = `
{
  "id": {
    "topic": "json-topic",
    "type": "float64"
  },
  "asyncStream": true,
  "asyncReceiver": false,
  "sorted": true
}
`
				v, err := pubsub.StreamDescriptionFromJSON([]byte(jsonStr))
				Expect(err).To(BeNil())
				Expect(v.Asynchronous).To(BeTrue())
				Expect(v.AutoCleanup).To(BeFalse())
				Expect(v.Sort).To(BeTrue())
			})
		})

		Context("Validation", func() {
			It("fails when ID is missing", func() {
				var yml = `
asyncStream: true
`
				_, err := pubsub.StreamDescriptionFromYML([]byte(yml))
				Expect(err).To(Equal(pubsub.ErrStreamDescriptionWithoutID))
			})
		})
	})

})
