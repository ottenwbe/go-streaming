package streams_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/streams"
)

var _ = Describe("Descriptions", func() {

	Describe("Streams Description", func() {
		Context("Parsing", func() {
			It("it can be parsed correctly", func() {

				var yml = `
name: test
id: 3c191d62-6574-4951-a8e6-4ec83c947250
async: true
`
				//yml := "name: test\nid:\n  3c191d62-6574-4951-a8e6-4ec83c947250"
				v, err := streams.StreamDescriptionFromYML([]byte(yml))
				Expect(v.Name).To(Equal("test"))
				Expect(v.ID).To(Equal(uuid.MustParse("3c191d62-6574-4951-a8e6-4ec83c947250")))
				Expect(v.Async).To(Equal(true))
				Expect(err).To(BeNil())
			})
		})
		It("creates an ID automatically if not provided", func() {
			var yml = `
name: test2
async: true
`
			//yml := "name: test\nid:\n  3c191d62-6574-4951-a8e6-4ec83c947250"
			v, err := streams.StreamDescriptionFromYML([]byte(yml))
			Expect(v.Name).To(Equal("test2"))
			Expect(v.ID).NotTo(Equal(uuid.Nil))
			Expect(v.Async).To(Equal(true))
			Expect(err).To(BeNil())
		})
	})

	Describe("Create with Description", func() {
		It("registers a Stream", func() {
			var yml = `
name: test
id: 3c191d62-6574-4951-a9e6-4ec83c947250
async: true
`
			d, _ := streams.StreamDescriptionFromYML([]byte(yml))
			streams.InstantiateStream(d)

			_, err := streams.PubSubSystem.GetStream(d.StreamID())
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

			streams.InstantiateStream(d)
			streams.DeleteStream(d)

			_, err := streams.PubSubSystem.GetStream(d.StreamID())
			Expect(err).To(Equal(streams.StreamNotFoundError()))

		})
	})
})
