package main

import (
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
	"go-stream-processing/pkg/query"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

var (
	numEvents = 100000
)

func main() {
	// define the query
	q, defErr := query.ContinuousGreater[int]("in", 50, "out")
	if defErr != nil {
		zap.S().Error("could not create query", zap.Error(defErr))
		return
	}

	// start the query
	qs, runErr := query.Run[int](q, defErr)
	if len(runErr) > 0 {
		zap.S().Error("could not run the query", zap.Errors("errors", runErr))
	}
	// always close your query when no longer needed to free resources
	defer query.Close(qs)

	publishEvents()
	receiveProcessedEvents(qs)

	// wait for some seconds to let streams being processed
	time.Sleep(time.Second * 10)
}

func receiveProcessedEvents(res *query.ResultSubscription[int]) {
	go func() {
		for {
			e := <-res.Notifier()
			zap.S().Infof("event received %v", e)
		}
	}()
}

func publishEvents() {
	go func() {
		for i := 0; i < numEvents; i++ {
			// create events in the range of  0-100
			if err := pubsub.Publish[int]("in", events.NewEvent[int](rand.Int()%100)); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
