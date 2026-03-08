package events_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var _ = Describe("SelectionPolicy", func() {
	Describe("CountingWindowPolicy", func() {
		Context("Select Events", func() {
			It("can read n events at the time", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](2, 2))
				defer b.StopBlocking()

				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(context.Background(), events.Arr(e1, e2, e3))

				es, _ := b.GetAndConsumeNextEvents(context.Background())

				Expect(es).To(Equal([]events.Event[string]{e1, e2}))
			})
			It("can select multiple events in a row", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](2, 1))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")
				e4 := events.NewEvent("e4")

				b.AddEvents(context.Background(), events.Arr(e1, e2, e3, e4))

				es1, _ := b.GetAndConsumeNextEvents(context.Background())
				es2, _ := b.GetAndConsumeNextEvents(context.Background())
				es3, _ := b.GetAndConsumeNextEvents(context.Background())

				Expect(es1).To(Equal([]events.Event[string]{e1, e2}))
				Expect(es2).To(Equal([]events.Event[string]{e2, e3}))
				Expect(es3).To(Equal([]events.Event[string]{e3, e4}))
			})
			It("waits until enough events are available", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](3, 3))
				defer b.StopBlocking()

				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				b.AddEvents(context.Background(), events.Arr(e1, e2))

				done := make(chan []events.Event[string])
				go func() {
					defer GinkgoRecover()
					res, _ := b.GetAndConsumeNextEvents(context.Background())
					done <- res
				}()

				Consistently(done).ShouldNot(Receive())

				b.AddEvent(context.Background(), events.NewEvent("e3"))
				Eventually(done).Should(Receive(HaveLen(3)))
			})
			It("handles shifts larger than size (skipping events)", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](1, 2))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(context.Background(), events.Arr(e1, e2, e3))

				es1, _ := b.GetAndConsumeNextEvents(context.Background())
				es2, _ := b.GetAndConsumeNextEvents(context.Background())

				Expect(es1).To(Equal([]events.Event[string]{e1}))
				Expect(es2).To(Equal([]events.Event[string]{e3}))
			})
			It("handles overlapping windows correctly", func() {
				b := events.NewConsumableAsyncBuffer[string](events.NewCountingWindowPolicy[string](3, 1))
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")
				e4 := events.NewEvent("e4")

				b.AddEvents(context.Background(), events.Arr(e1, e2, e3, e4))

				es1, _ := b.GetAndConsumeNextEvents(context.Background())
				es2, _ := b.GetAndConsumeNextEvents(context.Background())

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

				b.AddEvents(context.Background(), events.Arr[string](e1, e2, e3, e4))

				es1, _ := b.GetAndConsumeNextEvents(context.Background())
				es2, _ := b.GetAndConsumeNextEvents(context.Background())

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

				b.AddEvents(context.Background(), events.Arr[string](e1, e2, e3, e4, e5))

				es1, _ := b.GetAndConsumeNextEvents(context.Background())
				es2, _ := b.GetAndConsumeNextEvents(context.Background())
				es3, _ := b.GetAndConsumeNextEvents(context.Background())

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

				b.AddEvents(context.Background(), events.Arr[string](e1, e2, e3, e4))

				es1, _ := b.GetAndConsumeNextEvents(context.Background())
				es2, _ := b.GetAndConsumeNextEvents(context.Background())

				Expect(es1).To(Equal(events.Arr[string](e1, e2)))
				Expect(es2).To(Equal(events.Arr[string](e3)))
			})
		})
	})
	Describe("MultiTemporalWindowPolicy", func() {
		It("can select events across multiple buffers", func() {
			start := time.Now()
			// Window: [0, 10), Shift: 10
			p := events.NewMultiTemporalWindowPolicy[string](start, 10*time.Minute, 10*time.Minute)

			buf1 := events.NewEventBuffer[string]()
			buf2 := events.NewEventBuffer[string]()
			buffers := map[int]events.BufferReader[string]{0: buf1, 1: buf2}

			buf1.Add(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: start.Add(time.Minute)}, Content: "e1.1"})
			buf1.Add(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: start.Add(11 * time.Minute)}, Content: "e1.2"})
			buf1.Add(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: start.Add(21 * time.Minute)}, Content: "e1.3"})

			buf2.Add(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: start.Add(2 * time.Minute)}, Content: "e2.1"})
			buf2.Add(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: start.Add(12 * time.Minute)}, Content: "e2.2"})
			buf2.Add(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: start.Add(22 * time.Minute)}, Content: "e2.3"})

			p.UpdateSelection(buffers)
			Expect(p.NextSelectionReady(buffers)).To(BeTrue())
			sel := p.NextSelection()
			Expect(sel[0]).To(Equal(events.EventSelection{Start: 0, End: 0}))
			Expect(sel[1]).To(Equal(events.EventSelection{Start: 0, End: 0}))

			p.Shift()
			p.UpdateSelection(buffers)
			Expect(p.NextSelectionReady(buffers)).To(BeTrue())
			sel = p.NextSelection()
			Expect(sel[0]).To(Equal(events.EventSelection{Start: 1, End: 1}))
			Expect(sel[1]).To(Equal(events.EventSelection{Start: 1, End: 1}))
		})
	})
	Describe("DuoTemporalWindowPolicy", func() {
		It("can select events across two typed buffers", func() {
			start := time.Now()
			p := events.NewDuoTemporalWindowPolicy[string, int](start, 10*time.Minute, 10*time.Minute)

			bufLeft := events.NewEventBuffer[string]()
			bufRight := events.NewEventBuffer[int]()

			bufLeft.Add(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: start.Add(time.Minute)}, Content: "e1"})
			bufRight.Add(&events.TemporalEvent[int]{Stamp: events.TimeStamp{StartTime: start.Add(2 * time.Minute)}, Content: 1})

			p.UpdateSelection(bufLeft, bufRight)
			Expect(p.NextSelectionReady(bufLeft, bufRight)).To(BeFalse())

			bufLeft.Add(&events.TemporalEvent[string]{Stamp: events.TimeStamp{StartTime: start.Add(11 * time.Minute)}, Content: "e2"})
			bufRight.Add(&events.TemporalEvent[int]{Stamp: events.TimeStamp{StartTime: start.Add(12 * time.Minute)}, Content: 2})

			p.UpdateSelection(bufLeft, bufRight)
			Expect(p.NextSelectionReady(bufLeft, bufRight)).To(BeTrue())

			l, r := p.NextSelection()
			Expect(l).To(Equal(events.EventSelection{Start: 0, End: 0}))
			Expect(r).To(Equal(events.EventSelection{Start: 0, End: 0}))

			fired := false
			p.AddCallback(func(l []events.Event[string], r []events.Event[int]) { fired = true })
			p.UpdateSelection(bufLeft, bufRight)
			Expect(fired).To(BeTrue())
		})
	})
	Describe("SelectNextPolicy", func() {
		Context("Select Events", func() {
			It("one at a time", func() {
				b := events.NewConsumableAsyncBuffer(events.NewSelectNextPolicy[string]())
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(context.Background(), events.Arr(e1, e2, e3))

				es, _ := b.GetAndConsumeNextEvents(context.Background())

				Expect(es).To(Equal(events.Arr(e1)))
			})
			It("selects multiple events in a row", func() {
				b := events.NewConsumableAsyncBuffer(events.NewSelectNextPolicy[string]())
				e1 := events.NewEvent("e1")
				e2 := events.NewEvent("e2")
				e3 := events.NewEvent("e3")

				b.AddEvents(context.Background(), []events.Event[string]{e1, e2, e3})

				es1, _ := b.GetAndConsumeNextEvents(context.Background())
				es2, _ := b.GetAndConsumeNextEvents(context.Background())
				es3, _ := b.GetAndConsumeNextEvents(context.Background())

				Expect(es1).To(Equal(events.Arr(e1)))
				Expect(es2).To(Equal(events.Arr(e2)))
				Expect(es3).To(Equal(events.Arr(e3)))
			})
		})
	})
	Describe("SelectionPolicyConfig", func() {
		It("can create a CountingWindowPolicy", func() {
			desc := events.SelectionPolicyConfig{
				Type:  events.CountingWindow,
				Size:  5,
				Slide: 1,
			}
			p, err := events.NewSelectionPolicyFromConfig[int](desc)
			Expect(err).To(BeNil())
			Expect(p).NotTo(BeNil())
		})
		It("can create a SelectNextPolicy", func() {
			desc := events.SelectionPolicyConfig{
				Type: events.SelectNext,
			}
			p, err := events.NewSelectionPolicyFromConfig[int](desc)
			Expect(err).To(BeNil())
			Expect(p).NotTo(BeNil())
		})
		It("can create a TemporalWindowPolicy", func() {
			desc := events.SelectionPolicyConfig{
				Type:         events.TemporalWindow,
				WindowStart:  time.Now(),
				WindowLength: time.Minute,
				WindowShift:  time.Minute,
			}
			p, err := events.NewSelectionPolicyFromConfig[int](desc)
			Expect(err).To(BeNil())
			Expect(p).NotTo(BeNil())
		})
		It("can create a MultiTemporalWindowPolicy", func() {
			desc := events.SelectionPolicyConfig{
				Type:         events.MultiTemporalWindow,
				WindowStart:  time.Now(),
				WindowLength: time.Minute,
				WindowShift:  time.Minute,
			}
			p, err := events.NewMultiSelectionPolicyFromConfig[int](desc)
			Expect(err).To(BeNil())
			Expect(p).NotTo(BeNil())
		})
		It("can create a DuoTemporalWindowPolicy", func() {
			desc := events.SelectionPolicyConfig{
				Type:         events.DuoTemporalWindow,
				WindowStart:  time.Now(),
				WindowLength: time.Minute,
				WindowShift:  time.Minute,
			}
			p, err := events.NewDuoSelectionPolicyFromConfig[int, string](desc)
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
			desc := events.SelectionPolicyConfig{Type: events.CountingWindow, Size: 5, Slide: 1}
			jsonBytes, err := desc.ToJSON()
			Expect(err).To(BeNil())
			Expect(string(jsonBytes)).To(ContainSubstring(`"type":"counting"`))
		})
		It("can be marshalled to YAML", func() {
			desc := events.SelectionPolicyConfig{Type: events.CountingWindow, Size: 5, Slide: 1}
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
			_, err = events.NewSelectionPolicyFromConfig[int](desc)
			Expect(err).To(HaveOccurred())
		})
		It("returns error for unknown multi policy type", func() {
			desc := events.SelectionPolicyConfig{Type: "unknown"}
			_, err := events.NewMultiSelectionPolicyFromConfig[int](desc)
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, events.ErrUnknownMultiPolicyType)).To(BeTrue())
		})
		It("returns error for unknown duo policy type", func() {
			desc := events.SelectionPolicyConfig{Type: "unknown"}
			_, err := events.NewDuoSelectionPolicyFromConfig[int, string](desc)
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, events.ErrUnknownDuoPolicyType)).To(BeTrue())
		})
	})
	Describe("EventSelection", func() {
		It("validates ranges correctly", func() {
			Expect(events.EventSelection{Start: 0, End: 0}.IsValid()).To(BeTrue())
			Expect(events.EventSelection{Start: 0, End: 1}.IsValid()).To(BeTrue())
			Expect(events.EventSelection{Start: 1, End: 0}.IsValid()).To(BeFalse())
			Expect(events.EventSelection{Start: -1, End: 0}.IsValid()).To(BeFalse())
			Expect(events.EventSelection{Start: 0, End: -1}.IsValid()).To(BeFalse())
		})
	})
	Describe("Policy Internals", func() {
		Context("CountingWindowPolicy", func() {
			It("correctly offsets indices", func() {
				p := events.NewCountingWindowPolicy[int](2, 1)
				buf := events.NewEventBuffer[int]()
				buf.Add(events.NewEvent(1))
				buf.Add(events.NewEvent(2))
				buf.Add(events.NewEvent(3))

				// [0, 1]
				p.UpdateSelection(buf)
				Expect(p.NextSelection()).To(Equal(events.EventSelection{Start: 0, End: 1}))

				// Shift -> [1, 2]
				p.Shift()
				p.UpdateSelection(buf)
				Expect(p.NextSelection()).To(Equal(events.EventSelection{Start: 1, End: 2}))

				// Offset by 1
				p.Offset(1)
				Expect(p.NextSelection()).To(Equal(events.EventSelection{Start: 0, End: 1}))
			})

			It("provides correct description", func() {
				p := events.NewCountingWindowPolicy[int](5, 2)
				desc := p.Description()
				Expect(desc.Type).To(Equal(events.CountingWindow))
				Expect(desc.Size).To(Equal(5))
				Expect(desc.Slide).To(Equal(2))
			})

			It("provides correct description for SelectNext", func() {
				p := events.NewSelectNextPolicy[int]()
				desc := p.Description()
				Expect(desc.Type).To(Equal(events.SelectNext))
			})
		})

		Context("TemporalWindowPolicy", func() {
			It("correctly offsets indices", func() {
				start := time.Now()
				p := events.NewTemporalWindowPolicy[int](start, time.Minute, time.Minute)

				buf := events.NewEventBuffer[int]()
				buf.Add(&events.TemporalEvent[int]{Stamp: events.TimeStamp{StartTime: start.Add(time.Second)}})
				buf.Add(&events.TemporalEvent[int]{Stamp: events.TimeStamp{StartTime: start.Add(10 * time.Second)}})

				p.UpdateSelection(buf)
				Expect(p.NextSelection()).To(Equal(events.EventSelection{Start: 0, End: 1}))

				p.Offset(1)
				Expect(p.NextSelection()).To(Equal(events.EventSelection{Start: -1, End: 0}))
			})

			It("skips events before window start", func() {
				start := time.Now()
				p := events.NewTemporalWindowPolicy[int](start, time.Minute, time.Minute)

				buf := events.NewEventBuffer[int]()
				// Before window
				buf.Add(&events.TemporalEvent[int]{Stamp: events.TimeStamp{StartTime: start.Add(-time.Second)}})
				// Inside window
				buf.Add(&events.TemporalEvent[int]{Stamp: events.TimeStamp{StartTime: start.Add(time.Second)}})

				p.UpdateSelection(buf)
				// Should select [1, 1]
				Expect(p.NextSelection()).To(Equal(events.EventSelection{Start: 1, End: 1}))
			})
		})

		Context("MultiTemporalWindowPolicy", func() {
			It("correctly offsets indices for specific buffer", func() {
				start := time.Now()
				p := events.NewMultiTemporalWindowPolicy[int](start, time.Minute, time.Minute)

				buf0 := events.NewEventBuffer[int]()
				buf0.Add(&events.TemporalEvent[int]{Stamp: events.TimeStamp{StartTime: start.Add(time.Second)}})

				buffers := map[int]events.BufferReader[int]{0: buf0}

				p.UpdateSelection(buffers)
				p.Offset(0, 1)
				sel := p.NextSelection()
				Expect(sel[0]).To(Equal(events.EventSelection{Start: -1, End: -1}))
			})
		})
	})
})
