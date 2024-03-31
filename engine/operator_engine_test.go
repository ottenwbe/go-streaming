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

var _ = Describe("Add Operator", func() {

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

	inStream := &engine.DoubleStreamInput[engine.DoubleInputSelection[int, int], int, int]{
		Subscription1: input["a"],
		Subscription2: input["b"],
	}

	op := engine.NewOperator[engine.DoubleInputSelection[int, int], int](engine.Add[int], inStream, streamC)

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

var _ = Describe("Convert Operator", func() {
	Context("convert", func() {
		It("should change the type", func() {

			streamIn := streams.NewLocalSyncStream[int](streams.NewStreamDescription("a", uuid.New(), false))
			streamIn.Start()
			defer streamIn.Stop()

			streamOut := streams.NewLocalSyncStream[float32](streams.NewStreamDescription("b", uuid.New(), false))
			streamOut.Start()
			defer streamOut.Stop()

			inputSub := &engine.OperatorStreamSubscription[int]{
				Stream:      streamIn.Subscribe(),
				InputBuffer: buffer.NewSimpleAsyncBuffer[int](),
			}

			inputSub.Run()
			res := streamOut.Subscribe()

			inStream := &engine.SingleStreamInput1[events.Event[int], int]{
				Subscription: inputSub,
			}

			id := engine.NewOperator[events.Event[int], float32](engine.Convert[int, float32], inStream, streamOut)

			id.Start()

			event := events.NewEvent(8)

			streamIn.Publish(event)
			result := <-res.Notify

			r := result.GetContent()

			Expect(r).To(Equal(float32(8.0)))
		})
	})
})

var _ = Describe("Sum Operator", func() {
	Context("when executed", func() {
		It("should sum all values over a window", func() {

			streamIn := streams.NewLocalSyncStream[int](streams.NewStreamDescription("int values", uuid.New(), false))
			streamIn.Start()
			defer streamIn.Stop()

			streamOut := streams.NewLocalSyncStream[int](streams.NewStreamDescription("summed up values", uuid.New(), false))
			streamOut.Start()
			defer streamOut.Stop()

			inputSub := &engine.OperatorStreamSubscription[int]{
				Stream:      streamIn.Subscribe(),
				InputBuffer: buffer.NewConsumableAsyncBuffer[int](buffer.NewSelectNPolicy[int](2)),
			}

			inputSub.Run()
			res := streamOut.Subscribe()

			inStream := &engine.SingleStreamInputN[engine.SingleStreamSelectionN[int], int]{
				Subscription: inputSub,
			}

			sum := engine.NewOperator[engine.SingleStreamSelectionN[int], int](engine.BatchSum[int], inStream, streamOut)
			sum.Start()

			event := events.NewEvent(10)
			event1 := events.NewEvent(10)
			event2 := events.NewEvent(15)
			event3 := events.NewEvent(15)

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

var _ = Describe("Count Operator", func() {
	Context("when executed", func() {
		It("should sum all values over a window", func() {

			streamIn := streams.NewLocalSyncStream[float32](streams.NewStreamDescription("int values", uuid.New(), false))
			streamIn.Start()
			defer streamIn.Stop()

			streamOut := streams.NewLocalSyncStream[int](streams.NewStreamDescription("summed up values", uuid.New(), false))
			streamOut.Start()
			defer streamOut.Stop()

			inputSub := &engine.OperatorStreamSubscription[float32]{
				Stream:      streamIn.Subscribe(),
				InputBuffer: buffer.NewConsumableAsyncBuffer[float32](buffer.NewSelectNPolicy[float32](2)),
			}

			inputSub.Run()
			res := streamOut.Subscribe()

			inStream := &engine.SingleStreamInputN[engine.SingleStreamSelectionN[float32], float32]{
				Subscription: inputSub,
			}

			sum := engine.NewOperator[engine.SingleStreamSelectionN[float32], int](engine.BatchCount[float32, int], inStream, streamOut)
			sum.Start()

			event := events.NewEvent[float32](1.0)
			event1 := events.NewEvent[float32](1.1)
			event2 := events.NewEvent[float32](1.2)
			event3 := events.NewEvent[float32](1.3)

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
