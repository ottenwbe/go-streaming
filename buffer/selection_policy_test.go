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
				b := buffer.NewAsyncBuffer()
				e1, _ := events.NewEvent("e1")
				e2, _ := events.NewEvent("e2")
				e3, _ := events.NewEvent("e3")

				b.AddEvents([]events.Event{e1, e2, e3})

				s := buffer.SelectNPolicy{
					N: 2,
				}

				es := s.Apply(b)

				Expect(es).To(Equal([]events.Event{e1, e2}))
			})
		})
	})
	Describe("SelectNextPolicy", func() {
		Context("Select Events", func() {
			It("one at a time", func() {
				b := buffer.NewAsyncBuffer()
				e1, _ := events.NewEvent("e1")
				e2, _ := events.NewEvent("e2")
				e3, _ := events.NewEvent("e3")

				b.AddEvents([]events.Event{e1, e2, e3})

				s := buffer.SelectNextPolicy{}

				es := s.Apply(b)

				Expect(es).To(Equal([]events.Event{e1}))
			})
		})
	})
})
