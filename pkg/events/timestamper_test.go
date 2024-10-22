package events_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"

	"go-stream-processing/pkg/events"
)

var _ = Describe("Timestamper", func() {
	Context("MaxTimeStamp", func() {
		It("can be determined from a list of timestamps", func() {

			t1 := time.Now().Add(time.Hour)
			t2 := time.Now().Add(time.Minute)

			tm := events.MaxTimeStamp(t1, t2)
			Expect(tm).To(Equal(t1))
		})
	})
})
