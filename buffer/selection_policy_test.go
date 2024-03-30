package buffer_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/buffer"
	"go-stream-processing/events"
)

var _ = Describe("SelectionPolicy", func() {
	Describe("SelectNPolicy", func() {
		Context("Select Events", func() {
			It("can read n events at the time", func() {
				b := buffer.NewConsumableAsyncBuffer(buffer.NewSelectNPolicy(2))
				e1 := events.NewEvent("key", "e1")
				e2 := events.NewEvent("key", "e2")
				e3 := events.NewEvent("key", "e3")

				b.AddEvents([]events.Event{e1, e2, e3})

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal([]events.Event{e1, e2}))
			})
		})
	})
	Describe("SelectNextPolicy", func() {
		Context("Select Events", func() {
			It("one at a time", func() {
				b := buffer.NewConsumableAsyncBuffer(buffer.NewSelectNextPolicy())
				e1 := events.NewEvent("key", "e1")
				e2 := events.NewEvent("key", "e2")
				e3 := events.NewEvent("key", "e3")

				b.AddEvents([]events.Event{e1, e2, e3})

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal([]events.Event{e1}))
			})
		})
		Context("Select Events", func() {
			It("selects multiple events in a row", func() {
				b := buffer.NewConsumableAsyncBuffer(buffer.NewSelectNextPolicy())
				e1 := events.NewEvent("key", "e1")
				e2 := events.NewEvent("key", "e2")
				e3 := events.NewEvent("key", "e3")

				b.AddEvents([]events.Event{e1, e2, e3})

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()
				es3 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal([]events.Event{e1}))
				Expect(es2).To(Equal([]events.Event{e2}))
				Expect(es3).To(Equal([]events.Event{e3}))
			})
		})
	})
})
