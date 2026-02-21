package main

import (
	"math/rand"
	"time"

	"github.com/ottenwbe/go-streaming/pkg/engine"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/query"
	"go.uber.org/zap"
)

var (
	numEvents = 100000
)

func main() {
	// define the query
	q, defErr := engine.ContinuousGreater[int]("in", "out", 50)
	if defErr != nil {
		zap.S().Error("could not create query", zap.Error(defErr))
		return
	}

	// start the query
	qs, runErr := query.RunAndSubscribe[int](q, defErr)
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

func receiveProcessedEvents(res *query.TypedContinuousQuery[int]) {
	go func() {
		for {
			// wait until the next event notification arrives
			e, _ := res.Next()
			zap.S().Infof("event received %v", e)
		}
	}()
}

func publishEvents() {
	go func() {
		for i := 0; i < numEvents; i++ {
			// create events in the range of  0-100
			if err := pubsub.InstantPublishByTopic[int]("in", rand.Int()%100); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
