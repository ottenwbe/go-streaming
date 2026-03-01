package main

import (
	"sync"
	"time"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/log"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"go.uber.org/zap"
)

func main() {
	topic := "temporal-window-example"

	// 1. Define a Temporal Policy: 500ms windows
	// Events will be grouped into batches based on their timestamp.
	// A batch is emitted when an event arrives that is outside the current window.
	windowSize := 500 * time.Millisecond
	policy := events.MakeSelectionPolicy(events.TemporalWindowOption(time.Now(), windowSize, windowSize))

	var wg sync.WaitGroup
	wg.Add(2) // We expect 2 completed windows

	// 2. Subscribe with batch handling
	sub, err := pubsub.SubscribeBatchByTopic[int](
		topic,
		func(batch ...events.Event[int]) {
			zap.S().Infof("Received time-window batch of %d events", len(batch))
			for _, e := range batch {
				zap.S().Infof(" - Event: %d (Time: %s)", e.GetContent(), e.GetStamp().StartTime.Format("15:04:05.000"))
			}
			wg.Done()
		},
		pubsub.SubscriberWithSelectionPolicy(policy),
	)
	if err != nil {
		zap.S().Fatal(err)
	}
	defer pubsub.Unsubscribe(sub)

	// 3. Publish events for Window 1
	zap.S().Info("Publishing events for Window 1...")
	_ = pubsub.InstantPublishByTopic(topic, 1)
	time.Sleep(100 * time.Millisecond)
	_ = pubsub.InstantPublishByTopic(topic, 2)

	// 4. Sleep to cross the window boundary
	time.Sleep(600 * time.Millisecond)

	// 5. Triggering Window 1 flush
	zap.S().Info("Publishing event for Window 2 (flushes Window 1)...")
	_ = pubsub.InstantPublishByTopic(topic, 3)

	// 6. Sleep to cross the next window boundary
	time.Sleep(600 * time.Millisecond)

	// 7. Triggering Window 2 flush
	zap.S().Info("Publishing event for Window 3 (flushes Window 2)...")
	_ = pubsub.InstantPublishByTopic(topic, 4)

	wg.Wait()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
	log.SetLogger(zap.S())
}
