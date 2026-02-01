package events_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var _ = Describe("TimeStamp", func() {
	Context("String", func() {
		It("returns a readable string representation of the timestamp", func() {
			tMeta := events.StampMeta{"test": "test"}
			then := time.Date(
				2024, 01, 01, 12, 00, 00, 0, time.UTC)
			tm := events.TimeStamp{
				StartTime: then,
				EndTime:   then,
				Meta:      tMeta,
			}
			Expect(tm.String()).To(Equal("{\"start_time\":\"2024-01-01T12:00:00Z\",\"end_time\":\"2024-01-01T12:00:00Z\",\"meta\":{\"test\":\"test\"}}"))
		})
	})
	Context("Context", func() {
		It("returns a readable string representation of the timestamp", func() {
			tMeta := events.StampMeta{"test": "test"}
			then := time.Date(
				2024, 01, 01, 12, 00, 00, 0, time.UTC)
			tm := events.TimeStamp{
				StartTime: then,
				EndTime:   then,
				Meta:      tMeta,
			}
			Expect(tm.Content()).To(HaveKeyWithValue("start_time", then))
			Expect(tm.Content()).To(HaveKeyWithValue("test", "test"))
		})
	})
})
