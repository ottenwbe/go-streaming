package buffer_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	buffer2 "go-stream-processing/internal/buffer"
	"go-stream-processing/internal/events"
	"time"
)

var _ = Describe("SelectionPolicy", func() {
	Describe("CountingWindowPolicy", func() {
		Context("Select Events", func() {
			It("can read n events at the time", func() {
				b := buffer2.NewConsumableAsyncBuffer[string](buffer2.NewCountingWindowPolicy[string](2, 2))
				defer b.StopBlocking()

				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(events.Arr(e1, e2, e3))

				fmt.Printf("Added events in test %v\n", b.Len())

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal([]events.Event[string]{e1, e2}))
			})
			It("can select multiple events in a row", func() {
				b := buffer2.NewConsumableAsyncBuffer[string](buffer2.NewCountingWindowPolicy[string](2, 1))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")
				e4 := events.NewEvent("e4")

				b.AddEvents(events.Arr(e1, e2, e3, e4))

				fmt.Printf("Added events in test %v\n", b.Len())

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()
				es3 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal([]events.Event[string]{e1, e2}))
				Expect(es2).To(Equal([]events.Event[string]{e2, e3}))
				Expect(es3).To(Equal([]events.Event[string]{e3, e4}))
			})
		})
	})
	Describe("TemporalWindowPolicy", func() {
		Context("Select Events", func() {
			It("can select and slide based on time", func() {

				e1 := &events.TemporalEvent[string]{
					TimeStamp: time.Now(),
					Content:   "e1",
				}
				e2 := &events.TemporalEvent[string]{
					TimeStamp: e1.TimeStamp.Add(time.Minute * 10),
					Content:   "e2",
				}
				e3 := &events.TemporalEvent[string]{
					TimeStamp: e1.TimeStamp.Add(time.Minute * 65),
					Content:   "e3",
				}
				e4 := &events.TemporalEvent[string]{
					TimeStamp: e1.TimeStamp.Add(time.Hour * 24),
					Content:   "e4",
				}

				w := buffer2.NewTemporalWindowPolicy[string](e1.GetTimestamp(), time.Hour, time.Minute*10)
				b := buffer2.NewConsumableAsyncBuffer(w)

				b.AddEvents(events.Arr[string](e1, e2, e3, e4))

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal(events.Arr[string](e1, e2)))
				Expect(es2).To(Equal(events.Arr[string](e2, e3)))
			})
			It("can have empty windows", func() {

				e1 := &events.TemporalEvent[string]{
					TimeStamp: time.Now(),
					Content:   "e1",
				}
				e2 := &events.TemporalEvent[string]{
					TimeStamp: e1.TimeStamp.Add(time.Minute * 10),
					Content:   "e2",
				}
				e3 := &events.TemporalEvent[string]{
					TimeStamp: e1.TimeStamp.Add(time.Minute * 12),
					Content:   "e3",
				}
				e4 := &events.TemporalEvent[string]{
					TimeStamp: e1.TimeStamp.Add(time.Minute * 75),
					Content:   "e4",
				}
				e5 := &events.TemporalEvent[string]{
					TimeStamp: e1.TimeStamp.Add(time.Hour * 75),
					Content:   "e5",
				}

				w := buffer2.NewTemporalWindowPolicy[string](e1.GetTimestamp(), time.Minute*30, time.Minute*30)
				b := buffer2.NewConsumableAsyncBuffer(w)

				b.AddEvents(events.Arr[string](e1, e2, e3, e4, e5))

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()
				es3 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal(events.Arr[string](e1, e2, e3)))
				Expect(es2).To(Equal(events.Arr[string]()))
				Expect(es3).To(Equal(events.Arr[string](e4)))
			})
		})
	})
	Describe("SelectNextPolicy", func() {
		Context("Select Events", func() {
			It("one at a time", func() {
				b := buffer2.NewConsumableAsyncBuffer(buffer2.NewSelectNextPolicy[string]())
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(events.Arr(e1, e2, e3))

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal(events.Arr(e1)))
			})
			It("selects multiple events in a row", func() {
				b := buffer2.NewConsumableAsyncBuffer(buffer2.NewSelectNextPolicy[string]())
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents([]events.Event[string]{e1, e2, e3})

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()
				es3 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal(events.Arr(e1)))
				Expect(es2).To(Equal(events.Arr(e2)))
				Expect(es3).To(Equal(events.Arr(e3)))
			})
		})
	})
})
