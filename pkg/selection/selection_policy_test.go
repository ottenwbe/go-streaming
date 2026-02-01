package selection_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/internal/buffer"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

var _ = Describe("Policy", func() {
	Describe("CountingWindowPolicy", func() {
		Context("Select Events", func() {
			It("can read n events at the time", func() {
				b := buffer.NewConsumableAsyncBuffer[string](selection.NewCountingWindowPolicy[string](2, 2))
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
				b := buffer.NewConsumableAsyncBuffer[string](selection.NewCountingWindowPolicy[string](2, 1))
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
					Stamp: events.TimeStamp{
						StartTime: time.Now(),
						EndTime:   time.Now(),
						Meta:      map[string]interface{}{},
					},
					Content: "e1",
				}
				e2 := &events.TemporalEvent[string]{
					Stamp: events.TimeStamp{
						StartTime: e1.Stamp.StartTime.Add(time.Minute * 10),
						EndTime:   e1.Stamp.StartTime.Add(time.Minute * 10),
						Meta:      map[string]interface{}{},
					},
					Content: "e2",
				}
				e3 := &events.TemporalEvent[string]{
					Stamp: events.TimeStamp{
						StartTime: e1.Stamp.StartTime.Add(time.Minute * 65),
						EndTime:   e1.Stamp.StartTime.Add(time.Minute * 65),
						Meta:      map[string]interface{}{},
					},
					Content: "e3",
				}
				e4 := &events.TemporalEvent[string]{
					Stamp: events.TimeStamp{
						StartTime: e1.Stamp.StartTime.Add(time.Hour * 24),
						EndTime:   e1.Stamp.StartTime.Add(time.Hour * 24),
						Meta:      map[string]interface{}{},
					},
					Content: "e4",
				}

				w := selection.NewTemporalWindowPolicy[string](e1.GetStamp().StartTime, time.Hour, time.Minute*10)
				b := buffer.NewConsumableAsyncBuffer(w)

				b.AddEvents(events.Arr[string](e1, e2, e3, e4))

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal(events.Arr[string](e1, e2)))
				Expect(es2).To(Equal(events.Arr[string](e2, e3)))
			})
			It("can have empty windows", func() {

				e1 := &events.TemporalEvent[string]{
					Stamp: events.TimeStamp{
						StartTime: time.Now(),
						EndTime:   time.Now(),
						Meta:      map[string]interface{}{},
					},
					Content: "e1",
				}
				e2 := &events.TemporalEvent[string]{
					Stamp: events.TimeStamp{
						StartTime: e1.GetStamp().StartTime.Add(time.Minute * 10),
						EndTime:   e1.GetStamp().StartTime.Add(time.Minute * 10),
						Meta:      map[string]interface{}{},
					},
					Content: "e2",
				}
				e3 := &events.TemporalEvent[string]{
					Stamp: events.TimeStamp{
						StartTime: e1.GetStamp().StartTime.Add(time.Minute * 12),
						EndTime:   e1.GetStamp().StartTime.Add(time.Minute * 12),
						Meta:      map[string]interface{}{},
					},
					Content: "e3",
				}
				e4 := &events.TemporalEvent[string]{
					Stamp: events.TimeStamp{
						StartTime: e1.GetStamp().StartTime.Add(time.Minute * 75),
						EndTime:   e1.GetStamp().StartTime.Add(time.Minute * 75),
						Meta:      map[string]interface{}{},
					},
					Content: "e4",
				}
				e5 := &events.TemporalEvent[string]{
					Stamp: events.TimeStamp{
						StartTime: e1.GetStamp().StartTime.Add(time.Hour * 75),
						EndTime:   e1.GetStamp().StartTime.Add(time.Hour * 75),
						Meta:      map[string]interface{}{},
					},
					Content: "e5",
				}

				w := selection.NewTemporalWindowPolicy[string](e1.GetStamp().StartTime, time.Minute*30, time.Minute*30)
				b := buffer.NewConsumableAsyncBuffer(w)

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
				b := buffer.NewConsumableAsyncBuffer(selection.NewSelectNextPolicy[string]())
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(events.Arr(e1, e2, e3))

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal(events.Arr(e1)))
			})
			It("selects multiple events in a row", func() {
				b := buffer.NewConsumableAsyncBuffer(selection.NewSelectNextPolicy[string]())
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
