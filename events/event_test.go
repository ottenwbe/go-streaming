package events_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/events"
)

var _ = Describe("PubSub", func() {

	Context("Temporal events", func() {
		It("can be created and values can be read afterwards", func() {
			e := events.NewEvent("a", 1)
			Expect(e.GetContent("a")).To(Equal(1))
		})
	})
	Context("Temporal events", func() {
		It("can be created with a map as input", func() {
			m := map[string]interface{}{"key": 1}
			e := events.NewEventMap(m)
			Expect(e.GetContent("key")).To(Equal(1))
		})
	})
})
