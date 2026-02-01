package pubsub_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var _ = Describe("Descriptions", func() {

	Describe("Stream Description", func() {
		Context("Parsing YAML", func() {
			It("can be parsed correctly with all fields", func() {

				var yml = `
---
id: 
  topic: 3c191d62-6574-4951-a8e6-4ec83c947250
  type: string
asyncStream: true
asyncReceiver: true
singleFanIn: true
`
				v, err := pubsub.StreamDescriptionFromYML([]byte(yml))
				Expect(err).To(BeNil())
				Expect(v.ID).To(Equal(pubsub.MakeStreamID[string]("3c191d62-6574-4951-a8e6-4ec83c947250")))
				Expect(v.AsyncStream).To(BeTrue())
				Expect(v.AsyncReceiver).To(BeTrue())
				Expect(v.SingleFanIn).To(BeTrue())
			})

			It("parses correctly with default values", func() {
				var yml = `
id: 
  topic: test-defaults
  type: int
`
				v, err := pubsub.StreamDescriptionFromYML([]byte(yml))
				Expect(err).To(BeNil())
				Expect(v.AsyncStream).To(BeFalse())
				Expect(v.AsyncReceiver).To(BeFalse())
				Expect(v.SingleFanIn).To(BeFalse())
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
  "singleFanIn": true
}
`
				v, err := pubsub.StreamDescriptionFromJSON([]byte(jsonStr))
				Expect(err).To(BeNil())
				Expect(v.AsyncStream).To(BeTrue())
				Expect(v.AsyncReceiver).To(BeFalse())
				Expect(v.SingleFanIn).To(BeTrue())
			})
		})

		Context("Validation", func() {
			It("fails when ID is missing", func() {
				var yml = `
asyncStream: true
`
				_, err := pubsub.StreamDescriptionFromYML([]byte(yml))
				Expect(err).To(Equal(pubsub.StreamDescriptionWithoutID))
			})
		})
	})

})
