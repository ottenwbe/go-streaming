package events_test

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var _ = Describe("PubSub", func() {

	Context("Temporal events", func() {
		It("can be created and values can be read afterwards", func() {
			e := events.NewEvent(1)
			Expect(e.GetContent()).To(Equal(1))
		})
		It("can be created and metadata can be read afterwards", func() {
			e := events.NewEventM(1, events.StampMeta{"test": 1})
			Expect(e.GetStamp().Meta).To(HaveKeyWithValue("test", 1))
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
		It("can create an Array of events", func() {
			e1 := events.NewEvent(1)
			e2 := events.NewEvent(2)
			es := events.Arr(e1, e2)
			Expect(es).To(Equal([]events.Event[int]{e1, e2}))
		})
	})
	Context("Stamp an Event w/o context", func() {
		It("allows to read stamped information", func() {
			tMeta := events.StampMeta{"test": 1}

			tm := events.NewEventM[string]("", tMeta)

			Expect(tm.GetStamp().EndTime).To(Equal(tm.GetStamp().StartTime))
			Expect(tm.GetStamp().Meta).To(Equal(tMeta))
		})
	})
	Context("Stamp an Event w/ context", func() {
		It("allows to read stamped information w/o meta", func() {
			tMeta2 := map[string]interface{}{"test2": 2}
			tMeta3 := map[string]interface{}{"test3": 3}
			s2 := events.TimeStamp{
				StartTime: time.Now().Add(time.Hour),
				EndTime:   time.Now().Add(2 * time.Hour),
				Meta:      tMeta2,
			}
			e2 := &events.TemporalEvent[string]{
				Stamp:   s2,
				Content: "",
			}
			e3 := events.NewEventM[string]("", tMeta3)

			tm := events.NewEventFromOthers[string]("", e2, e3).GetStamp()

			Expect(tm.EndTime.After(tm.StartTime)).To(BeTrue())
			Expect(tm.EndTime).To(Equal(s2.EndTime))
			Expect(tm.Meta).To(HaveKeyWithValue("test2", 2))
			Expect(tm.Meta).To(HaveKeyWithValue("test3", 3))
		})
		It("allows to read stamped information w/ meta", func() {
			tMeta1 := map[string]interface{}{"test": 1}
			tMeta2 := map[string]interface{}{"test2": 2}
			tMeta3 := map[string]interface{}{"test3": 3}
			s2 := events.TimeStamp{
				StartTime: time.Now().Add(time.Hour),
				EndTime:   time.Now().Add(2 * time.Hour),
				Meta:      tMeta2,
			}
			e2 := &events.TemporalEvent[string]{
				Stamp:   s2,
				Content: "",
			}
			e3 := events.NewEventM[string]("", tMeta3)

			tm := events.NewEventFromOthersM[string]("", tMeta1, e2, e3).GetStamp()

			Expect(tm.EndTime.After(tm.StartTime)).To(BeTrue())
			Expect(tm.EndTime).To(Equal(s2.EndTime))
			Expect(tm.Meta).To(HaveKeyWithValue("test", 1))
			Expect(tm.Meta).To(HaveKeyWithValue("test2", 2))
			Expect(tm.Meta).To(HaveKeyWithValue("test3", 3))
		})
	})
})
