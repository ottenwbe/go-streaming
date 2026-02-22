package main

import (
	"sync"

	"github.com/ottenwbe/go-streaming/pkg/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/query"

	"go.uber.org/zap"
)

const (
	maxEvents = 1000
)

func main() {
	var wg sync.WaitGroup

	q, err := query.Query[float32](
		query.Process[float32](
			engine.ContinuousConvert[int, float32](),
			//	query.OnStream[int](
			query.Process[int](
				engine.ContinuousGreater[int](50),
				query.FromSourceStream[int]("in", pubsub.WithAsynchronousStream(true)),
			),
			//),
		),
	)
	if err != nil {
		zap.S().Fatal(err)
	}
	q.Subscribe(func(e events.Event[float32]) {
		zap.S().Infof("event received: %v", e.GetContent())
	})
	q.Run()

	sid, _ := pubsub.GetOrAddStream[int]("in")
	startPublisher(sid, &wg)

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

func unregister(streamID pubsub.StreamID, publisher pubsub.Publisher[int]) {
	err := pubsub.UnRegisterPublisher(publisher)
	if err != nil {
		zap.S().Fatalf("Publisher could not be unregistered from %v", streamID)
	}
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
