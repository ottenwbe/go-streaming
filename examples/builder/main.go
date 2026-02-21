package main

import (
	"math/rand"

	"github.com/ottenwbe/go-streaming/internal/engine"
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

	policy := selection.NewCountingWindowPolicy[float64](10, shift)
	q, err := query.NewBuilder().
		Stream(query.S[float64]("in", pubsub.WithAsynchronousStream(true))).
		Query(engine.ContinuousBatchSum("in", "out", policy)).
		Build()

	// start the continuous query
	qs, _ := query.RunAndSubscribe[float64](q, err...)
	// always close your query when no longer needed to free resources
	defer query.Close(qs)

	publishEvents()
	waitForProcessedEvents(qs)
}

func waitForProcessedEvents(res *query.TypedContinuousQuery[float64]) {
	for i := 0; i < numEvents/shift; i++ {
		if e, more := res.Next(); more {
			zap.S().Infof("event received %v", e[0])
		}
	}
}

func publishEvents() {
	go func() {
		for i := 0; i < numEvents; i++ {
			if err := pubsub.InstantPublishByTopic("in", events.NewEvent(rand.Float64())); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
		}
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
