package main

import (
	"fmt"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
)

func main() {
	// 1. Define and register a stream
	topic := "greetings"
	// Stream of strings, synchronous (false), multiple publishers allowed (false for singleFanIn)
	desc := pubsub.MakeStreamDescription[string](topic, false, false)
	pubsub.AddOrReplaceStreamFromDescription[string](desc)

	// 2. Subscribe
	// MakeStreamID creates a typed ID for the topic
	streamID := pubsub.MakeStreamID[string](topic)
	receiver, _ := pubsub.Subscribe[string](streamID)

	// 3. Publish
	publisher, _ := pubsub.RegisterPublisher[string](streamID)
	publisher.Publish(events.NewEvent("Hello World!"))

	// 4. Consume
	event := <-receiver.Notify()
	fmt.Printf("Received: %s\n", event.GetContent())
}
