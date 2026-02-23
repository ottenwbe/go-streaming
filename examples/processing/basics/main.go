package main

import (
	"math/rand"
	"time"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/processing"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"go.uber.org/zap"
)

var (
	numEvents = 100
)

func main() {
	// define the query
	q, err := processing.Query[int](
		processing.Process[int](
			processing.ContinuousGreater[int](50),
			processing.FromSourceStream[int]("in", pubsub.WithAsynchronousStream(true)),
		),
	)
	if err != nil {
		zap.S().Fatal("could not create query", zap.Error(err))
	}

	// Subscribe to the output
	err = q.Subscribe(func(e events.Event[int]) {
		zap.S().Infof("event received %v", e)
	})
	if err != nil {
		zap.S().Fatal("could not subscribe", zap.Error(err))
	}

	// start the query
	if err := q.Run(); err != nil {
		zap.S().Fatal("could not run the query", zap.Error(err))
	}
	// always close your query when no longer needed to free resources
	defer processing.Close(q)

	publishEvents()

	// wait for some seconds to let streams being processed
	time.Sleep(time.Second * 2)
}

func publishEvents() {
	go func() {
		for i := 0; i < numEvents; i++ {
			// create events in the range of  0-100
			if err := pubsub.InstantPublishByTopic[int]("in", rand.Int()%100); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
