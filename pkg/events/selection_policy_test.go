package events_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var _ = Describe("Policy", func() {
	Describe("CountingWindowPolicy", func() {
		Context("Select Events", func() {
			It("can read n events at the time", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](2, 2))
				defer b.StopBlocking()

				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(events.Arr(e1, e2, e3))

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal([]events.Event[string]{e1, e2}))
			})
			It("can select multiple events in a row", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](2, 1))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")
				e4 := events.NewEvent("e4")

				b.AddEvents(events.Arr(e1, e2, e3, e4))

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()
				es3 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal([]events.Event[string]{e1, e2}))
				Expect(es2).To(Equal([]events.Event[string]{e2, e3}))
				Expect(es3).To(Equal([]events.Event[string]{e3, e4}))
			})
			It("waits until enough events are available", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](3, 3))
				defer b.StopBlocking()

				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				b.AddEvents(events.Arr(e1, e2))

				done := make(chan []events.Event[string])
				go func() {
					defer GinkgoRecover()
					done <- b.GetAndConsumeNextEvents()
				}()

				Consistently(done).ShouldNot(Receive())

				b.AddEvent(events.NewEvent("e3"))
				Eventually(done).Should(Receive(HaveLen(3)))
			})
			It("handles shifts larger than size (skipping events)", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](1, 2))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(events.Arr(e1, e2, e3))

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal([]events.Event[string]{e1}))
				Expect(es2).To(Equal([]events.Event[string]{e3}))
			})
			It("handles overlapping windows correctly", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](3, 1))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")
				e4 := events.NewEvent("e4")

				b.AddEvents(events.Arr(e1, e2, e3, e4))

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal([]events.Event[string]{e1, e2, e3}))
				Expect(es2).To(Equal([]events.Event[string]{e2, e3, e4}))
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

				w := events.NewTemporalWindowPolicy[string](e1.GetStamp().StartTime, time.Hour, time.Minute*10)
				b := events.NewConsumableAsyncBuffer(w)

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

				w := events.NewTemporalWindowPolicy[string](e1.GetStamp().StartTime, time.Minute*30, time.Minute*30)
				b := events.NewConsumableAsyncBuffer(w)

				b.AddEvents(events.Arr[string](e1, e2, e3, e4, e5))

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()
				es3 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal(events.Arr[string](e1, e2, e3)))
				Expect(es2).To(Equal(events.Arr[string]()))
				Expect(es3).To(Equal(events.Arr[string](e4)))
			})
			It("handles events on window boundaries", func() {
				startTime := time.Now()

				e1 := &events.TemporalEvent[string]{
					Stamp:   events.TimeStamp{StartTime: startTime},
					Content: "e1",
				}
				e2 := &events.TemporalEvent[string]{
					Stamp:   events.TimeStamp{StartTime: startTime.Add(time.Minute * 5)},
					Content: "e2",
				}
				e3 := &events.TemporalEvent[string]{
					Stamp:   events.TimeStamp{StartTime: startTime.Add(time.Minute * 10)},
					Content: "e3",
				}
				e4 := &events.TemporalEvent[string]{
					Stamp:   events.TimeStamp{StartTime: startTime.Add(time.Hour * 24)},
					Content: "e4",
				}

				w := events.NewTemporalWindowPolicy[string](startTime, time.Minute*10, time.Minute*10)
				b := events.NewConsumableAsyncBuffer(w)

				b.AddEvents(events.Arr[string](e1, e2, e3, e4))

				es1 := b.GetAndConsumeNextEvents()
				es2 := b.GetAndConsumeNextEvents()

				Expect(es1).To(Equal(events.Arr[string](e1, e2)))
				Expect(es2).To(Equal(events.Arr[string](e3)))
			})
		})
	})
	Describe("SelectNextPolicy", func() {
		Context("Select Events", func() {
			It("one at a time", func() {
				b := events.NewConsumableAsyncBuffer(events.NewSelectNextPolicy[string]())
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(events.Arr(e1, e2, e3))

				es := b.GetAndConsumeNextEvents()

				Expect(es).To(Equal(events.Arr(e1)))
			})
			It("selects multiple events in a row", func() {
				b := events.NewConsumableAsyncBuffer(events.NewSelectNextPolicy[string]())
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
	Describe("PolicyDescription", func() {
		It("can create a CountingWindowPolicy", func() {
			desc := events.PolicyDescription{
				Type:  events.CountingWindow,
				Size:  5,
				Slide: 1,
			}
			p, err := events.NewPolicyFromDescription[int](desc)
			Expect(err).To(BeNil())
			Expect(p).NotTo(BeNil())
		})
		It("can create a SelectNextPolicy", func() {
			desc := events.PolicyDescription{
				Type: events.SelectNext,
			}
			p, err := events.NewPolicyFromDescription[int](desc)
			Expect(err).To(BeNil())
			Expect(p).NotTo(BeNil())
		})
		It("can create a TemporalWindowPolicy", func() {
			desc := events.PolicyDescription{
				Type:         events.TemporalWindow,
				WindowStart:  time.Now(),
				WindowLength: time.Minute,
				WindowShift:  time.Minute,
			}
			p, err := events.NewPolicyFromDescription[int](desc)
			Expect(err).To(BeNil())
			Expect(p).NotTo(BeNil())
		})
		It("can be parsed from JSON", func() {
			jsonStr := `{"active":true,"type":"counting","size":5,"slide":1}`
			desc, err := events.PolicyDescriptionFromJSON([]byte(jsonStr))
			Expect(err).To(BeNil())
			Expect(desc.Type).To(Equal(events.CountingWindow))
			Expect(desc.Size).To(Equal(5))
			Expect(desc.Slide).To(Equal(1))
		})
		It("can be parsed from YAML", func() {
			ymlStr := `
active: true
type: counting
size: 5
slide: 1
`
			desc, err := events.PolicyDescriptionFromYML([]byte(ymlStr))
			Expect(err).To(BeNil())
			Expect(desc.Type).To(Equal(events.CountingWindow))
			Expect(desc.Size).To(Equal(5))
			Expect(desc.Slide).To(Equal(1))
		})
		It("can be marshalled to JSON", func() {
			desc := events.PolicyDescription{Type: events.CountingWindow, Size: 5, Slide: 1}
			jsonBytes, err := desc.ToJSON()
			Expect(err).To(BeNil())
			Expect(string(jsonBytes)).To(ContainSubstring(`"type":"counting"`))
		})
		It("can be marshalled to YAML", func() {
			desc := events.PolicyDescription{Type: events.CountingWindow, Size: 5, Slide: 1}
			ymlBytes, err := desc.ToYML()
			Expect(err).To(BeNil())
			Expect(string(ymlBytes)).To(ContainSubstring("type: counting"))
		})
		It("returns error for invalid JSON", func() {
			_, err := events.PolicyDescriptionFromJSON([]byte(`{invalid-json`))
			Expect(err).To(HaveOccurred())
		})
		It("returns error for unknown policy type", func() {
			jsonStr := `{"type":"unknown_type"}`
			desc, err := events.PolicyDescriptionFromJSON([]byte(jsonStr))
			Expect(err).To(BeNil())
			_, err = events.NewPolicyFromDescription[int](desc)
			Expect(err).To(HaveOccurred())
		})
	})
})
