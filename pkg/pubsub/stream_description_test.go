package pubsub_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/pkg/pubsub"
)

var _ = Describe("Descriptions", func() {

	Describe("streams Description", func() {
		Context("Parsing", func() {
			It("it can be parsed correctly", func() {

				var yml = `
id: 
  topic: 3c191d62-6574-4951-a8e6-4ec83c947250
  type: string
async: true
`
				//yml := "name: test\nid:\n  3c191d62-6574-4951-a8e6-4ec83c947250"
				v, err := pubsub.StreamDescriptionFromYML([]byte(yml))
				Expect(v.ID).To(Equal(pubsub.MakeStreamID[string]("3c191d62-6574-4951-a8e6-4ec83c947250")))
				//Expect(v.ID).To(Equal(uuid.MustParse("3c191d62-6574-4951-a8e6-4ec83c947250")))
				Expect(v.Async).To(Equal(true))
				Expect(err).To(BeNil())
			})
		})
		It("creates an ID automatically if not provided", func() {
			var yml = `
id: 
  topic: test2
  type: string
async: true
`
			//yml := "name: test\nid:\n  3c191d62-6574-4951-a8e6-4ec83c947250"
			v, err := pubsub.StreamDescriptionFromYML([]byte(yml))
			//Expect(v.Name).To(Equal("test2"))

			Expect(v.ID).To(Equal(pubsub.MakeStreamID[string]("test2")))
			Expect(v.Async).To(Equal(true))
			Expect(err).To(BeNil())
		})
	})

})
