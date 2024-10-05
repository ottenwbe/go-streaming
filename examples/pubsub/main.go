package pubsub

import (
	"fmt"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
)

func main() {
	streamConfig := pubsub.MakeStreamDescription[int]("int stream", false)
	if intStream, err := pubsub.AddOrReplaceStreamD[int](streamConfig); err != nil {
		fmt.Printf("intStream could not be created: %v", err)
	}

	go func() {
		s,err := pubsub.Subscribe[int]("int stream")
	}()

	go func() {
		intStream. <- events.NewEvent(1)
	}()

}
