package main

import (
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
	"go-stream-processing/pkg/query"
	"go-stream-processing/pkg/selection"
	"go.uber.org/zap"
	"math/rand"
)

var (
	numEvents = 100000
	shift     = 10
)

func main() {

	policy := selection.NewCountingWindowPolicy[float64](10, shift)
	q, err := query.NewBuilder().
		Stream(query.S[float64]("in", true)).
		Query(query.ContinuousBatchSum[float64]("in", "out", policy)).
		Build()

	// start the continuous query
	qs, _ := query.Run[float64](q, err...)

	// always close your query when no longer needed to free resources
	defer query.Close(qs)

	publishEvents()
	waitForProcessedEvents(qs)
}

func waitForProcessedEvents(res *query.TypedContinuousQuery[float64]) {
	for i := 0; i < numEvents/shift; i++ {
		if e, more := res.Notify(); more {
			zap.S().Infof("event received %v", e)
		}
	}
}

func publishEvents() {
	go func() {
		for i := 0; i < numEvents; i++ {
			if err := pubsub.PublishByTopic[float64]("in", events.NewEvent[float64](rand.Float64())); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
