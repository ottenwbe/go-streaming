package buffer_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
)

var _ = Describe("SelectionPolicy", func() {
	Describe("CountingWindowPolicy", func() {
		Context("Select Events", func() {
			It("can read n events at the time", func() {
				b := buffer.NewConsumableAsyncBuffer[string](buffer.NewSelectNPolicy[string](2, 2))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(events.Arr(e1, e2, e3))

				fmt.Printf("Added events in test %v\n", b.Len())

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal([]events.Event[string]{e1, e2}))
			})
			It("can select multiple events in a row", func() {
				b := buffer.NewConsumableAsyncBuffer[string](buffer.NewSelectNPolicy[string](2, 1))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")
				e4 := events.NewEvent("e4")

				b.AddEvents(events.Arr(e1, e2, e3, e4))

				fmt.Printf("Added events in test %v\n", b.Len())

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal([]events.Event[string]{e1, e2}))
				Expect(es2).To(Equal([]events.Event[string]{e2, e3}))
			})
		})
	})
	Describe("SelectNextPolicy", func() {
		Context("Select Events", func() {
			It("one at a time", func() {
				b := buffer.NewConsumableAsyncBuffer(buffer.NewSelectNextPolicy[string]())
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(events.Arr(e1, e2, e3))

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal(events.Arr(e1)))
			})
			It("selects multiple events in a row", func() {
				b := buffer.NewConsumableAsyncBuffer(buffer.NewSelectNextPolicy[string]())
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents([]events.Event[string]{e1, e2, e3})

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()
				es3 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal([]events.Event[string]{e1}))
				Expect(es2).To(Equal([]events.Event[string]{e2}))
				Expect(es3).To(Equal([]events.Event[string]{e3}))
			})
		})
	})
})
