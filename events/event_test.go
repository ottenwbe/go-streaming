package events_test

import (
	"encoding/json"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/events"
)

var _ = Describe("PubSub", func() {

	Context("Temporal events", func() {
		It("can be created and values can be read afterwards", func() {
			e := events.NewEvent(1)
			Expect(e.GetContent()).To(Equal(1))
		})
		It("can be created with a map as input", func() {
			m := map[string]interface{}{"key": 1}
			e := events.NewEvent(m)
			Expect(e.GetContent()["key"]).To(Equal(1))
		})
		It("can be created from a json", func() {
			b, _ := json.Marshal(map[string]interface{}{"key": 1})
			e, _ := events.NewEventFromJSON(b)
			Expect(e.GetContent()["key"]).To(Equal(1.0))
		})
		It("can create an Array", func() {
			e1 := events.NewEvent(1)
			e2 := events.NewEvent(2)
			es := events.Arr(e1, e2)
			Expect(es).To(Equal([]events.Event[int]{e1, e2}))
		})
	})
})
