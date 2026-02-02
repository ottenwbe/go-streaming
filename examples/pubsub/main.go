package main

import (
	"time"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	"go.uber.org/zap"
)

func main() {

	var (
		intStream pubsub.StreamID
		err       error
	)

	streamConfig := pubsub.MakeStreamDescription[int]("int stream", pubsub.WithSingleFanIn(true))
	if intStream, err = pubsub.AddOrReplaceStreamFromDescription[int](streamConfig); err != nil {
		zap.S().Errorf("intStream could not be created: %v", err)
	}

	startSubscriber1(intStream)

	startSubscriber2(intStream)

	publisher(intStream)

	// wait for some seconds to let streams being processed
	time.Sleep(time.Second * 1)
}

func publisher(intStream pubsub.StreamID) {
	publisher, _ := pubsub.RegisterPublisher[int](intStream)

	for i := 0; i < 1000; i++ {
		zap.S().Infof("Now sending: %v", i)
		_ = publisher.Publish(events.NewEvent(i))
	}
}

func startSubscriber2(intStream pubsub.StreamID) {
	go func() {
		s, _ := pubsub.SubscribeByTopicID[int](intStream)
		for {
			e, _ := s.Consume()
			zap.S().Infof("event received by sub1: %v", e)
		}
	}()
}

func startSubscriber1(intStream pubsub.StreamID) {
	go func() {
		s, _ := pubsub.SubscribeByTopicID[int](intStream)
		for {
			e, _ := s.Consume()
			zap.S().Infof("event received by sub2: %v", e)
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
