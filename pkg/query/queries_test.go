package query_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	buffer2 "go-stream-processing/internal/buffer"
	"go-stream-processing/internal/engine"
	"go-stream-processing/internal/events"
	streams2 "go-stream-processing/internal/streams"
	query2 "go-stream-processing/pkg/query"
)

var _ = Describe("Add Operator1", func() {

	Context("execute the operator", func() {
		It("should correctly perform the operation on streams", func() {

			stream, c := query2.QueryAdd[int]("test-add-in1", "test-add-in2", "test-add-out")
			defer query2.Close(c)

			res := stream.Subscribe()
			event := events.NewEvent(8)
			event2 := events.NewEvent(3)

			streamA, _ := streams2.GetStreamN[int]("test-add-in1")
			streamB, _ := streams2.GetStreamN[int]("test-add-in2")

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

			stream, _ := query2.QueryConvert[int, float32]("convert-test-in", "convert-test-out")
			res := stream.Subscribe()
			event := events.NewEvent(8)

			streamIn, _ := streams2.GetStreamN[int]("convert-test-in")

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

			selection := buffer2.NewCountingWindowPolicy[int](2, 2)

			stream, _ := query2.QueryBatchSum[int]("int values", "sum values", selection)
			res := stream.Subscribe()

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(15)

			streamIn, _ := streams2.GetStreamN[int]("int values")
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

			selection := buffer2.NewCountingWindowPolicy[float32](2, 2)
			stream, _ := query2.QueryBatchCount[float32, int]("countable floats", "counted floats", selection)
			res := stream.Subscribe()

			event := events.NewEvent[float32](1.0)
			event1 := events.NewEvent[float32](1.1)
			event2 := events.NewEvent[float32](1.2)
			event3 := events.NewEvent[float32](1.3)

			streamIn, _ := streams2.GetStreamN[float32]("countable floats")
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

			stream, _ := query2.QuerySmaller[int]("q-s-1", "res-s-1", 11)
			res := stream.Subscribe()
			streamIn, _ := streams2.GetStreamN[int]("q-s-1")

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

			resStream, _ := query2.QueryGreater("test-greater-11", 11, "test-greater-11-out")
			res := resStream.Subscribe()

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(35)

			streamIn, _ := streams2.GetStreamN[int]("test-greater-11")
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

var _ = Describe("OperatorRepository", func() {

	var (
		op engine.OperatorControl
	)

	BeforeEach(func() {
		streamIn := streams2.NewLocalSyncStream[int](streams2.NewStreamDescription("int values", uuid.New(), false))
		streamOut := streams2.NewLocalSyncStream[int](streams2.NewStreamDescription("summed up values", uuid.New(), false))

		inputSub := &engine.OperatorStreamSubscription[int]{
			Stream:      streamIn.Subscribe(),
			InputBuffer: buffer2.NewConsumableAsyncBuffer[int](buffer2.NewCountingWindowPolicy[int](2, 2)),
		}

		inStream := &engine.SingleStreamInput1[engine.SingleStreamSelection1[int], int]{
			Subscription: inputSub,
		}

		smaller := func(input engine.SingleStreamSelection1[int]) []events.Event[int] {
			if input.GetContent() < 11 {
				return []events.Event[int]{input}
			} else {
				return []events.Event[int]{}
			}

		}

		op = engine.NewOperatorN[engine.SingleStreamSelection1[int], int](smaller, inStream, streamOut)
	})

	Context("Get and put", func() {
		It("adds new operators to the map and retrieves it", func() {
			err := query2.OperatorRepository().Put(op)
			oResult, ok := query2.OperatorRepository().Get(op.ID())

			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())
			Expect(op.ID()).To(Equal(oResult.ID()))
		})
		It("does not allow duplicated operators", func() {
			query2.OperatorRepository().Put(op)
			err := query2.OperatorRepository().Put(op)
			Expect(err).ToNot(BeNil())
		})
		It("does not allow nil operators", func() {
			err := query2.OperatorRepository().Put(nil)
			Expect(err).ToNot(BeNil())
		})
	})
	Context("List", func() {
		It("can be listed", func() {
			query2.OperatorRepository().Put(op)
			l := query2.OperatorRepository().List()
			Expect(l).To(ContainElement(op))
		})
	})
})
