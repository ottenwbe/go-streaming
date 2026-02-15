package main

//package main
//
//import (
//	"fmt"
//
//	"github.com/ottenwbe/go-streaming/pkg/events"
//	"github.com/ottenwbe/go-streaming/pkg/pubsub"
//)
//
//func main() {
//	c := make(chan string)
//
//	go func(s chan string) {
//		for i := range 100 {
//			s <- fmt.Sprintf("a%v", i)
//		}
//	}(c)
//
//	go func(s chan string) {
//		for i := range 100 {
//			s <- fmt.Sprintf("%v", i)
//		}
//	}(c)
//
//	go func(s chan string) {
//		a := <-s
//		fmt.Println(a)
//	}(c)
//
//}
//
//func test1() {
//	// 1. Define and register a stream
//	topic := "greetings"
//	// stream of strings, synchronous (false), multiple publishers allowed (false for singleFanIn)
//	desc := pubsub.MakeStreamDescription[string](topic, pubsub.WithAsyncStream(true))
//	sID, _ := pubsub.AddOrReplaceStreamFromDescription[string](desc)
//	defer pubsub.ForceRemoveStream(sID)
//
//	started := make(chan bool)
//	finished := make(chan bool)
//
//	// MakeStreamID creates a typed ID for the topic
//	streamID := pubsub.MakeStreamID[string](topic)
//
//	go func(finished chan bool) {
//		// 2. SubscribeByTopicID
//		receiver, _ := pubsub.SubscribeByTopicID[string](streamID)
//		started <- true
//		// 4. Consume the event
//		event, _ := receiver.Next()
//		fmt.Printf("Received: %s\n", event.GetContent())
//		finished <- true
//	}(finished)
//
//	<-started
//	// 3. Publish
//	publisher, _ := pubsub.RegisterPublisher[string](streamID)
//	publisher.Publish(events.NewEvent("Hello World!"))
//	<-finished
//}
