package api_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/api"
)

var _ = Describe("API", func() {

	Describe("Streams Description", func() {
		Context("Parsing", func() {
			It("it can be parsed correctly", func() {

				var yml = `
name: test
id: 3c191d62-6574-4951-a8e6-4ec83c947250
async: true
`
				//yml := "name: test\nid:\n  3c191d62-6574-4951-a8e6-4ec83c947250"
				v, err := api.StreamDescriptionFromYML([]byte(yml))
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
			v, err := api.StreamDescriptionFromYML([]byte(yml))
			Expect(v.Name).To(Equal("test2"))
			Expect(v.ID).NotTo(Equal(uuid.Nil))
			Expect(v.Async).To(Equal(true))
			Expect(err).To(BeNil())
		})
	})
})
