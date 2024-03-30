package engine_test

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/buffer"
	"go-stream-processing/engine"
	"go-stream-processing/events"
	"go-stream-processing/streams"
)

func test(eventMap map[string][]events.Event) []events.Event {
	var i, j int

	fmt.Println("op")

	i = eventMap["a"][0].GetContent("key").(int)
	j = eventMap["b"][0].GetContent("key").(int)

	fmt.Println("op done")

	e := events.NewEvent("key", i+j)
	return []events.Event{e}
}

var _ = Describe("Operator", func() {

	streamA := streams.NewLocalSyncStream(streams.NewStreamDescription("a", uuid.New(), false))
	streamA.Start()
	streamB := streams.NewLocalSyncStream(streams.NewStreamDescription("b", uuid.New(), false))
	streamB.Start()
	streamC := streams.NewLocalSyncStream(streams.NewStreamDescription("c", uuid.New(), false))
	streamC.Start()
	resRec := streamC.Subscribe()
	streams.PubSubSystem.NewOrReplaceStream(streamC)
	input := make(map[string]*engine.OperatorStreamSubscription)
	input["a"] = &engine.OperatorStreamSubscription{
		Stream:      streamA.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer(),
		Selection:   buffer.NewSelectNPolicy(1),
	}
	input["a"].Run()

	input["b"] = &engine.OperatorStreamSubscription{
		Stream:      streamB.Subscribe(),
		InputBuffer: buffer.NewSimpleAsyncBuffer(),
		Selection:   buffer.NewSelectNPolicy(1),
	}
	input["b"].Run()

	op := engine.NewOperator(test, input, []streams.StreamID{streamC.ID()})

	Describe("Operator", func() {
		Context("op1", func() {
			It("should be consumed", func() {
				event := events.NewEvent("key", 8)
				event2 := events.NewEvent("key", 3)

				fmt.Println("StartBlocking Operator")
				op.Start()
				fmt.Println("Started Operator")

				fmt.Println("Pub 0")
				streamA.Publish(event)
				fmt.Println("Pub 1")
				streamB.Publish(event2)
				fmt.Println("Pub 2")

				result := <-resRec.Notify

				r := result.GetContent("key")

				Expect(r).To(Equal(11))
			})
		})
	})
})
