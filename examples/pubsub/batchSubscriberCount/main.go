package main

import (
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/log"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"go.uber.org/zap"
)

func main() {
	topic := "batch-example"
	const (
		totalEvents = 150
		batchSize   = 5
	)

	// 1. Define a policy: ConnectTo events in batches of 5
	// This creates a "Counting Window" that triggers every 5 events.
	policy := events.MakeSelectionPolicy(events.CountingWindowOption(batchSize, batchSize))

	// The wg is just used to control the flow of the example, it is typically not needed for a realistic use case
	var wg sync.WaitGroup
	// We expect (totalEvents / batchSize) batches
	wg.Add(totalEvents / batchSize)

	// 2. Subscribe with batch handling
	sub, err := pubsub.SubscribeBatchByTopic[int](
		topic,
		func(batch ...events.Event[int]) {
			zap.S().Infof("Received batch of %d events: %v", len(batch), getContent(batch))
			wg.Done()
		},
		pubsub.SubscriberWithSelectionPolicy(policy),
	)
	if err != nil {
		zap.S().Fatal(err)
	}
	// ensure cleanup
	defer pubsub.Unsubscribe(sub)

	// 3. Publish events
	zap.S().Info("Publishing events...")
	for i := 1; i <= totalEvents; i++ {
		_ = pubsub.InstantPublishByTopic(topic, i)
	}

	// Wait for all batches to be delivered
	wg.Wait()
}

func getContent(batch []events.Event[int]) []int {
	content := make([]int, len(batch))
	for i, e := range batch {
		content[i] = e.GetContent()
	}
	return content
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
	log.SetLogger(zap.S())
}
