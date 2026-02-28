package main

import (
	"math/rand"
	"time"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/log"
	"github.com/ottenwbe/go-streaming/pkg/processing"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"go.uber.org/zap"
)

var (
	numEvents = 100
)

func main() {
	log.SetLogger(zap.S())
	// Define the query using the Fluent Builder API
	// This constructs a pipeline: Source("in") -> GreaterThan(0.5) -> Output
	b := processing.NewBuilder[float64]()
	b.From(processing.Source[float64]("in", pubsub.WithAsynchronousStream(true))).
		Process(processing.Operator[float64](processing.Greater[float64](0.5)))

	q, err := b.Build(false)
	if err != nil {
		zap.S().Fatal(err)
	}

	// Subscribe to the query output
	err = q.Subscribe(func(e events.Event[float64]) {
		zap.S().Infof("event received: %v", e.GetContent())
	})
	if err != nil {
		zap.S().Fatal(err)
	}

	// Start the query execution
	if err := q.Run(); err != nil {
		zap.S().Fatal(err)
	}
	defer processing.Close(q)

	// PublishContent events to the input stream
	publishEvents()

	// Keep the application running to process events
	time.Sleep(2 * time.Second)
}

func publishEvents() {
	go func() {
		for i := 0; i < numEvents; i++ {
			// PublishContent raw float64 values; the system wraps them in Events
			if err := pubsub.InstantPublishByTopic("in", rand.Float64()); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
