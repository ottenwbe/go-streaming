package main

import (
	"sync"
	"time"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	"go.uber.org/zap"
)

const (
	maxEvents = 1000
)

func main() {
	var wg sync.WaitGroup

	// 1. Configure the publish/subscribe system for the topic 'Some Integers'
	intStreamID, err := pubsub.AddOrReplaceStream[int]("Some Integers", pubsub.WithAsynchronousStream(true), pubsub.WithSubscriberSync(false))
	if err != nil {
		zap.S().Fatalf("Failed to create stream: %v", err)
	}
	defer pubsub.TryRemoveStreams(intStreamID)

	// 2. Subscribe to the topic 'Some Integers'
	startSubscriber("Subscriber 1", intStreamID, 2*time.Microsecond)
	startSubscriber("Subscriber 2", intStreamID, time.Microsecond)

	// 3. PublishContent events to the topic 'Some Integers'
	startPublisher(intStreamID, &wg)

	// 4. Wait for publishers and subscribers to send and receive all events
	wg.Wait()
}

func startPublisher(streamID pubsub.StreamID, wg *sync.WaitGroup) {
	publisher, err := pubsub.RegisterPublisher[int](streamID)
	if err != nil {
		zap.S().Fatalf("Failed to register publisher: %v", err)
	}

	wg.Go(func() {
		// Unregister from stream, after the work is done
		defer unregister(streamID, publisher)

		for i := 0; i < maxEvents; i++ {
			zap.S().Infof("Now sending: %v", i)
			_ = publisher.Publish(events.NewEvent(i))
		}
	})
}

func startSubscriber(name string, streamID pubsub.StreamID, delay time.Duration) {
	_, err := pubsub.SubscribeByTopicID[int](streamID, func(e events.Event[int]) {
		zap.S().Infof("Event received by %s: %v", name, e)
		time.Sleep(delay)
	})
	if err != nil {
		zap.S().Fatalf("Failed to subscribe %s: %v", name, err)
	}
}

func unregister(streamID pubsub.StreamID, publisher pubsub.Publisher[int]) {
	err := pubsub.UnRegisterPublisher(publisher)
	if err != nil {
		zap.S().Fatalf("Publisher could not be unregistered from %v", streamID)
	}
}

func unsubscribe(name string, streamID pubsub.StreamID, subscriber pubsub.Subscriber[int]) {
	err := pubsub.Unsubscribe(subscriber)
	if err != nil {
		zap.S().Fatalf("%v could not unsubscribe from %v", name, streamID)
	}
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
