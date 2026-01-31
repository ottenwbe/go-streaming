package main

import (
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
	"go.uber.org/zap"
	"time"
)

func main() {

	var (
		intStream pubsub.Stream
		err       error
	)

	streamConfig := pubsub.MakeStreamDescription[int]("int stream", false, false)
	if intStream, err = pubsub.AddOrReplaceStreamD[int](streamConfig); err != nil {
		zap.S().Errorf("intStream could not be created: %v", err)
	}

	startSubscriber1(intStream)

	startSubscriber2(intStream)

	publisher(intStream)

	// wait for some seconds to let streams being processed
	time.Sleep(time.Second * 1)
}

func publisher(intStream pubsub.Stream) {
	publisher, _ := pubsub.RegisterPublisher[int](intStream.ID())

	for i := 0; i < 1000; i++ {
		zap.S().Infof("Now sending: %v", i)
		_ = publisher.Publish(events.NewEvent(i))
	}
}

func startSubscriber2(intStream pubsub.Stream) {
	go func() {
		s, _ := pubsub.Subscribe[int](intStream.ID())
		for {
			e, _ := s.Consume()
			zap.S().Infof("event received by sub1: %v", e)
		}
	}()
}

func startSubscriber1(intStream pubsub.Stream) {
	go func() {
		s, _ := pubsub.Subscribe[int](intStream.ID())
		for {
			e, _ := s.Consume()
			zap.S().Infof("event received by sub2: %v", e)
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
