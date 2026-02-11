package query_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/query"
	"github.com/ottenwbe/go-streaming/pkg/selection"
)

var _ = Describe("Query Builder", func() {

	Context("Build", func() {
		It("creates a successful query", func() {
			b := query.NewBuilder().Stream(query.S[int]("builder1", pubsub.WithAsynchronousStream(true))).Query(query.ContinuousBatchSum[int]("builder1", "builder2", selection.NewCountingWindowPolicy[int](10, 10)))
			q, ok := b.Build()

			Expect(ok).To(BeNil())
			Expect(q).ToNot(BeNil())
		})
	})
})

var _ = Describe("Add Operator1", func() {

	Context("execute the operator", func() {
		It("should correctly perform the operation on pubsub", func() {

			c, err1 := query.ContinuousAdd[int]("test-add-in1", "test-add-in2", "test-add-out")
			qs, _ := query.RunAndSubscribe[int](c, err1)
			defer query.Close(qs)

			event := events.NewEvent(8)
			event2 := events.NewEvent(3)

			streamAID := pubsub.MakeStreamID[int]("test-add-in1")
			streamBID := pubsub.MakeStreamID[int]("test-add-in2")

			publisherA, _ := pubsub.RegisterPublisher[int](streamAID)
			publisherB, _ := pubsub.RegisterPublisher[int](streamBID)

			publisherA.Publish(event)
			publisherB.Publish(event2)

			result, _ := qs.Next()

			r := result[0].GetContent()

			Expect(r).To(Equal(11))
		})
	})
})

var _ = Describe("Convert Operator1", func() {
	Context("convert", func() {
		It("should change the type", func() {

			c, err1 := query.ContinuousConvert[int, float32]("convert-test-in", "convert-test-out")
			qs, _ := query.RunAndSubscribe[float32](c, err1)
			defer query.Close(qs)

			event := events.NewEvent(8)

			streamInID := pubsub.MakeStreamID[int]("convert-test-in")
			publisher, _ := pubsub.RegisterPublisher[int](streamInID)

			publisher.Publish(event)
			result, _ := qs.Next()

			r := result[0].GetContent()

			Expect(r).To(Equal(float32(8.0)))
		})
	})
})

var _ = Describe("Sum Operator1", func() {
	Context("when executed", func() {
		It("should sum all values over a window", func() {

			selection := selection.NewCountingWindowPolicy[int](2, 2)

			qs, _ := query.RunAndSubscribe[int](query.ContinuousBatchSum[int]("int values", "sum values", selection))
			defer query.Close(qs)

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(15)

			streamInID := pubsub.MakeStreamID[int]("int values")
			publisher, _ := pubsub.RegisterPublisher[int](streamInID)

			publisher.Publish(event)
			publisher.Publish(event1)
			publisher.Publish(event2)
			publisher.Publish(event3)

			result1, _ := qs.Next()
			result2, _ := qs.Next()

			r1 := result1[0].GetContent()
			r2 := result2[0].GetContent()

			Expect(r1).To(Equal(20))
			Expect(r2).To(Equal(30))
		})
	})
})

var _ = Describe("Count Operator1", func() {
	Context("when executed", func() {
		It("should sum all values over a window", func() {

			selection := selection.NewCountingWindowPolicy[float32](2, 2)
			qs, _ := query.RunAndSubscribe[int](query.ContinuousBatchCount[float32, int]("countable floats", "counted floats", selection))
			defer query.Close(qs)

			event := events.NewEvent[float32](1.0)
			event1 := events.NewEvent[float32](1.1)
			event2 := events.NewEvent[float32](1.2)
			event3 := events.NewEvent[float32](1.3)

			streamInID := pubsub.MakeStreamID[float32]("countable floats")
			publisher, _ := pubsub.RegisterPublisher[float32](streamInID)
			publisher.Publish(event)
			publisher.Publish(event1)
			publisher.Publish(event2)
			publisher.Publish(event3)

			result1, _ := qs.Next()
			result2, _ := qs.Next()

			r1 := result1[0].GetContent()
			r2 := result2[0].GetContent()

			Expect(r1).To(Equal(2))
			Expect(r2).To(Equal(2))
		})
	})
})

var _ = Describe("Smaller OperatorControl", func() {
	Context("when executed", func() {
		It("should remove large events", func() {

			qs, _ := query.RunAndSubscribe[int](query.ContinuousSmaller[int]("q-s-1", "res-s-1", 11))
			defer query.Close(qs)

			streamInID := pubsub.MakeStreamID[int]("q-s-1")

			event := events.NewEvent(9)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(35)

			publisher, _ := pubsub.RegisterPublisher[int](streamInID)

			publisher.Publish(event)
			publisher.Publish(event1)
			publisher.Publish(event2)
			publisher.Publish(event3)

			result1, _ := qs.Next()
			result2, _ := qs.Next()

			r1 := result1[0].GetContent()
			r2 := result2[0].GetContent()

			Expect(r1).To(Equal(9))
			Expect(r2).To(Equal(10))
		})
	})
})

var _ = Describe("Greater OperatorControl", func() {
	Context("when executed", func() {
		It("should remove small events", func() {

			qs, _ := query.RunAndSubscribe[int](query.ContinuousGreater("test-greater-11", "test-greater-11-out", 11))
			defer query.Close(qs)

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(35)

			streamInID := pubsub.MakeStreamID[int]("test-greater-11")
			publisher, _ := pubsub.RegisterPublisher[int](streamInID)
			publisher.Publish(event)
			publisher.Publish(event1)
			publisher.Publish(event2)
			publisher.Publish(event3)

			result1, _ := qs.Next()
			result2, _ := qs.Next()

			r1 := result1[0].GetContent()
			r2 := result2[0].GetContent()

			Expect(r1).To(Equal(15))
			Expect(r2).To(Equal(35))
		})
	})
})
