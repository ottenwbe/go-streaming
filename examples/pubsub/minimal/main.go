package main

import (
	"fmt"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

func main() {
	done := make(chan struct{})

	// 1. Subscribe to a topic
	sub, _ := pubsub.SubscribeByTopic[int]("my-topic",
		func(e events.Event[int]) {
			fmt.Printf("Received: %v\n", e.GetContent())
			close(done)
		})

	// 2. PublishContent to the same topic
	pub, _ := pubsub.RegisterPublisherByTopic[int]("my-topic")
	pub.PublishContent(42)

	// 3. Consume the event
	<-done

	// 4. Cleanup
	pubsub.UnRegisterPublisher(pub)
	pubsub.Unsubscribe(sub)
}
