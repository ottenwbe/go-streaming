package query_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/internal/buffer"
	"go-stream-processing/internal/events"
	"go-stream-processing/internal/pubsub"
	"go-stream-processing/pkg/query"
)

var _ = Describe("Add Operator1", func() {

	Context("execute the operator", func() {
		It("should correctly perform the operation on pubsub", func() {

			c, _ := query.ContinuousAdd[int]("test-add-in1", "test-add-in2", "test-add-out")
			c.Run()
			defer c.Close()

			res, _ := pubsub.Subscribe[int](c.Output.ID())
			event := events.NewEvent(8)
			event2 := events.NewEvent(3)

			streamA, _ := pubsub.GetStreamN[int]("test-add-in1")
			streamB, _ := pubsub.GetStreamN[int]("test-add-in2")

			streamA.Publish(event)
			streamB.Publish(event2)

			result := <-res.Notify

			r := result.GetContent()

			Expect(r).To(Equal(11))
		})
	})
})

var _ = Describe("Convert Operator1", func() {
	Context("convert", func() {
		It("should change the type", func() {

			c, _ := query.ContinuousConvert[int, float32]("convert-test-in", "convert-test-out")
			c.Run()
			defer c.Close()

			res, _ := pubsub.Subscribe[float32](c.Output.ID())
			event := events.NewEvent(8)

			streamIn, _ := pubsub.GetStreamN[int]("convert-test-in")

			streamIn.Publish(event)
			result := <-res.Notify

			r := result.GetContent()

			Expect(r).To(Equal(float32(8.0)))
		})
	})
})

var _ = Describe("Sum Operator1", func() {
	Context("when executed", func() {
		It("should sum all values over a window", func() {

			selection := buffer.NewCountingWindowPolicy[int](2, 2)

			c, _ := query.ContinuousBatchSum[int]("int values", "sum values", selection)
			c.Run()
			defer c.Close()

			res, _ := pubsub.Subscribe[int](c.Output.ID())

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(15)

			streamIn, _ := pubsub.GetStreamN[int]("int values")
			streamIn.Publish(event)
			streamIn.Publish(event1)
			streamIn.Publish(event2)
			streamIn.Publish(event3)

			result1 := <-res.Notify
			result2 := <-res.Notify

			r1 := result1.GetContent()
			r2 := result2.GetContent()

			Expect(r1).To(Equal(20))
			Expect(r2).To(Equal(30))
		})
	})
})

var _ = Describe("Count Operator1", func() {
	Context("when executed", func() {
		It("should sum all values over a window", func() {

			selection := buffer.NewCountingWindowPolicy[float32](2, 2)
			c, _ := query.ContinuousBatchCount[float32, int]("countable floats", "counted floats", selection)
			c.Run()
			defer c.Close()

			res, _ := pubsub.Subscribe[int](c.Output.ID())

			event := events.NewEvent[float32](1.0)
			event1 := events.NewEvent[float32](1.1)
			event2 := events.NewEvent[float32](1.2)
			event3 := events.NewEvent[float32](1.3)

			streamIn, _ := pubsub.GetStreamN[float32]("countable floats")
			streamIn.Publish(event)
			streamIn.Publish(event1)
			streamIn.Publish(event2)
			streamIn.Publish(event3)

			result1 := <-res.Notify
			result2 := <-res.Notify

			r1 := result1.GetContent()
			r2 := result2.GetContent()

			Expect(r1).To(Equal(2))
			Expect(r2).To(Equal(2))
		})
	})
})

var _ = Describe("Smaller OperatorControl", func() {
	Context("when executed", func() {
		It("should remove large events", func() {

			c, _ := query.ContinuousSmaller[int]("q-s-1", "res-s-1", 11)
			c.Run()
			defer c.Close()

			res, _ := pubsub.Subscribe[int](c.Output.ID())
			streamIn, _ := pubsub.GetStreamN[int]("q-s-1")

			event := events.NewEvent(9)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(35)

			streamIn.Publish(event)
			streamIn.Publish(event1)
			streamIn.Publish(event2)
			streamIn.Publish(event3)

			result1 := <-res.Notify
			result2 := <-res.Notify

			r1 := result1.GetContent()
			r2 := result2.GetContent()

			Expect(r1).To(Equal(9))
			Expect(r2).To(Equal(10))
		})
	})
})

var _ = Describe("Greater OperatorControl", func() {
	Context("when executed", func() {
		It("should remove small events", func() {

			c, _ := query.ContinuousGreater("test-greater-11", 11, "test-greater-11-out")
			c.Run()
			defer c.Close()

			res, _ := pubsub.Subscribe[int](c.Output.ID())

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(35)

			streamIn, _ := pubsub.GetStreamN[int]("test-greater-11")
			streamIn.Publish(event)
			streamIn.Publish(event1)
			streamIn.Publish(event2)
			streamIn.Publish(event3)

			result1 := <-res.Notify
			result2 := <-res.Notify

			r1 := result1.GetContent()
			r2 := result2.GetContent()

			Expect(r1).To(Equal(15))
			Expect(r2).To(Equal(35))
		})
	})
})
