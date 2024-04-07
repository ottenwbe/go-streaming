package engine_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/buffer"
	"go-stream-processing/engine"
	"go-stream-processing/events"
	"go-stream-processing/streams"
)

var _ = Describe("Add Operator1", func() {

	streamA := streams.NewLocalSyncStream[int](streams.NewStreamDescription("a", uuid.New(), false))
	streamA.Start()
	streamB := streams.NewLocalSyncStream[int](streams.NewStreamDescription("b", uuid.New(), false))
	streamB.Start()
	streamC := streams.NewLocalSyncStream[int](streams.NewStreamDescription("c", uuid.New(), false))
	streamC.Start()
	resRec := streamC.Subscribe()
	streams.NewOrReplaceStream[int](streamC)
	input := make(map[string]*engine.OperatorStreamSubscription[int])
	input["a"] = &engine.OperatorStreamSubscription[int]{
		Stream:      streamA.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer[int](),
	}
	input["a"].Run()

	input["b"] = &engine.OperatorStreamSubscription[int]{
		Stream:      streamB.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer[int](),
	}
	input["b"].Run()

	inStream := &engine.DoubleStreamInput1[engine.DoubleInputSelection1[int, int], int, int]{
		Subscription1: input["a"],
		Subscription2: input["b"],
	}

	op := engine.NewOperator[engine.DoubleInputSelection1[int, int], int](engine.Add[int], inStream, streamC)

	Context("execute the operator", func() {
		It("should correctly perform the operation on streams", func() {
			event := events.NewEvent(8)
			event2 := events.NewEvent(3)

			op.Start()

			streamA.Publish(event)
			streamB.Publish(event2)

			result := <-resRec.Notify

			r := result.GetContent()

			Expect(r).To(Equal(11))
		})
	})
})

var _ = Describe("Convert Operator1", func() {
	Context("convert", func() {
		It("should change the type", func() {

			res := engine.QueryConvert[int, float32]("convert-test-in", "convert-test-out").Subscribe()
			event := events.NewEvent(8)

			streamIn, _ := streams.GetStreamN[int]("convert-test-in")

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

			res := engine.QueryBatchSum[int]("int values", "sum values", selection).Subscribe()

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(15)

			streamIn, _ := streams.GetStreamN[int]("int values")
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
			res := engine.QueryBatchCount[float32, int]("countable floats", "counted floats", selection).Subscribe()

			event := events.NewEvent[float32](1.0)
			event1 := events.NewEvent[float32](1.1)
			event2 := events.NewEvent[float32](1.2)
			event3 := events.NewEvent[float32](1.3)

			streamIn, _ := streams.GetStreamN[float32]("countable floats")
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

			streamIn := streams.NewLocalSyncStream[int](streams.NewStreamDescription("int values", uuid.New(), false))
			streamIn.Start()
			defer streamIn.Stop()

			streamOut := streams.NewLocalSyncStream[int](streams.NewStreamDescription("summed up values", uuid.New(), false))
			streamOut.Start()
			defer streamOut.Stop()

			inputSub := &engine.OperatorStreamSubscription[int]{
				Stream:      streamIn.Subscribe(),
				InputBuffer: buffer.NewConsumableAsyncBuffer[int](buffer.NewCountingWindowPolicy[int](2, 2)),
			}

			inputSub.Run()
			res := streamOut.Subscribe()

			inStream := &engine.SingleStreamInput1[engine.SingleStreamSelection1[int], int]{
				Subscription: inputSub,
			}

			greater := engine.NewOperatorN[engine.SingleStreamSelection1[int], int](engine.Smaller[int](11), inStream, streamOut)
			greater.Start()

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

			res := engine.QueryGreater("test-greater-11", 11, "test-greater-11-out").Subscribe()

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(35)

			streamIn, _ := streams.GetStreamN[int]("test-greater-11")
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
		streamIn := streams.NewLocalSyncStream[int](streams.NewStreamDescription("int values", uuid.New(), false))
		streamOut := streams.NewLocalSyncStream[int](streams.NewStreamDescription("summed up values", uuid.New(), false))

		inputSub := &engine.OperatorStreamSubscription[int]{
			Stream:      streamIn.Subscribe(),
			InputBuffer: buffer.NewConsumableAsyncBuffer[int](buffer.NewCountingWindowPolicy[int](2, 2)),
		}

		inStream := &engine.SingleStreamInput1[engine.SingleStreamSelection1[int], int]{
			Subscription: inputSub,
		}

		op = engine.NewOperatorN(engine.Smaller[int](11), inStream, streamOut)
	})

	Context("Get and put", func() {
		It("adds new operators to the map and retrieves it", func() {
			err := engine.OperatorRepository().Put(op)
			oResult, ok := engine.OperatorRepository().Get(op.ID())

			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())
			Expect(op.ID()).To(Equal(oResult.ID()))
		})
		It("does not allow duplicated operators", func() {
			engine.OperatorRepository().Put(op)
			err := engine.OperatorRepository().Put(op)
			Expect(err).ToNot(BeNil())
		})
		It("does not allow nil operators", func() {
			err := engine.OperatorRepository().Put(nil)
			Expect(err).ToNot(BeNil())
		})
	})
	Context("List", func() {
		It("can be listed", func() {
			engine.OperatorRepository().Put(op)
			l := engine.OperatorRepository().List()
			Expect(l).To(ContainElement(op))
		})
	})
})
