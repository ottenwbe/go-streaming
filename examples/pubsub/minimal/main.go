package main

import (
	"fmt"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

func main() {
	// 1. Subscribe to a topic
	sub, _ := pubsub.SubscribeByTopic[int]("my-topic")

	// 2. Publish to the same topic
	pub, _ := pubsub.RegisterPublisherByTopic[int]("my-topic")
	pub.Publish(events.NewEvent(42))

	// 3. Consume the event
	event, _ := sub.Consume()
	fmt.Printf("Received: %v\n", event.GetContent())

	// 4. Cleanup
	pubsub.UnRegisterPublisher(pub)
	pubsub.Unsubscribe(sub)
}
