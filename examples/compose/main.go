package main

import (
	"math/rand"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"github.com/ottenwbe/go-streaming/pkg/query"
	"github.com/ottenwbe/go-streaming/pkg/selection"
	"go.uber.org/zap"
)

var (
	numEvents = 100000
	shift     = 10
)

func main() {
	q1, q2 := defineContinuousQueries()

	// merge the continuous queries and
	// start the composed query
	qs, err := query.RunAndSubscribe[int](q1.ComposeWith(q2))
	if err != nil {
		zap.S().Error("could not subscribe to query", zap.Errors("errors", err))
	}
	// always close your query when no longer needed to free resources
	defer query.Close(qs)

	publishEvents()
	receiveProcessedEvents(qs)
}

func defineContinuousQueries() (*query.ContinuousQuery, *query.ContinuousQuery) {
	// define the continuous query (or queries)
	// query 1 continuously sums up the (float64) contents of subsequent 10 events in stream in
	policy := selection.NewCountingWindowPolicy[float64](10, shift)
	q1, err := query.ContinuousBatchSum[float64]("in", "out", policy)
	if err != nil {
		zap.S().Error("could not create sum query", zap.Error(err))
	}
	// query 2 continuously converts float64 values in stream out to int
	q2, err := query.ContinuousConvert[float64, int]("out", "fin")
	if err != nil {
		zap.S().Error("could not create conversion query", zap.Error(err))
	}
	return q1, q2
}

func receiveProcessedEvents(res *query.TypedContinuousQuery[int]) {
	for i := 0; i < numEvents/shift; i++ {
		e, _ := res.Next()
		zap.S().Infof("event received %v", e)
	}
}

func publishEvents() {
	go func() {
		for i := 0; i < numEvents; i++ {
			if err := pubsub.InstantPublishByTopic[float64]("in", events.NewEvent[float64](rand.Float64())); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
