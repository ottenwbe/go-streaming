package main

import (
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
	"go-stream-processing/pkg/query"
	"go-stream-processing/pkg/selection"
	"go.uber.org/zap"
	"math/rand"
)

func main() {
	q1, q2 := defineContinuousQueries()

	// merge the continuous queries
	composedQ := q1.With(q2)
	// start the composedQ one
	composedQ.Run()
	// always close your query when no longer needed to free resources
	defer composedQ.Close()

	res, err := pubsub.Subscribe[int](composedQ.Output.ID())
	if err != nil {
		zap.S().Error("could not subscribe", zap.Error(err))
	}

	publishEvents(res)
	waitForProcessedEvents(res)
}

func defineContinuousQueries() (*query.ContinuousQuery, *query.ContinuousQuery) {
	// define the continuous query (or queries)
	policy := selection.NewCountingWindowPolicy[float64](10, 10)
	q1, err := query.ContinuousBatchSum[float64]("in", "out", policy)
	if err != nil {
		zap.S().Error("could not create query", zap.Error(err))
	}
	q2, err := query.ContinuousConvert[float64, int]("out", "fin")
	if err != nil {
		zap.S().Error("could not create query", zap.Error(err))
	}
	return q1, q2
}

func waitForProcessedEvents(res *pubsub.StreamReceiver[int]) {
	var fin = make(chan bool)
	go func() {
		for {
			if e, more := <-res.Notify; more {
				zap.S().Infof("event received %v", e)
			} else {
				fin <- true
			}
		}
	}()
	<-fin

}

func publishEvents(res *pubsub.StreamReceiver[int]) {
	go func() {
		for i := 0; i < 100000; i++ {
			if err := pubsub.PublishN[float64]("in", events.NewEvent[float64](rand.Float64())); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
		}
		// close channel
		pubsub.Unsubscribe[int](res)
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
