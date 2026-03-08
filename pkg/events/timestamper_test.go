package events_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var _ = Describe("TimeStamper", func() {
	Context("Merging Timestamps", func() {
		It("calculates the correct time span covering all input events", func() {
			now := time.Now()
			t1 := events.TimeStamp{StartTime: now.Add(1 * time.Hour), EndTime: now.Add(2 * time.Hour)}
			t2 := events.TimeStamp{StartTime: now, EndTime: now.Add(1 * time.Hour)}

			merged := events.NewEventFromOthers(0, t1, t2)
			Expect(merged.GetStamp().StartTime).To(Equal(now))
			Expect(merged.GetStamp().EndTime).To(Equal(now.Add(2 * time.Hour)))
		})

		It("merges metadata correctly", func() {
			t1 := events.TimeStamp{Meta: events.StampMeta{"a": 1}}
			t2 := events.TimeStamp{Meta: events.StampMeta{"b": 2}}

			merged := events.NewEventFromOthers(0, t1, t2)
			Expect(merged.GetStamp().Meta).To(HaveKeyWithValue("a", 1))
			Expect(merged.GetStamp().Meta).To(HaveKeyWithValue("b", 2))
		})
	})
})
