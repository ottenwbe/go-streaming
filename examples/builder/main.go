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

	policy := selection.NewCountingWindowPolicy[float64](10, 10)
	q, err := query.NewBuilder().
		Stream(query.S[float64]("in", true)).
		Query(query.ContinuousBatchSum[float64]("in", "out", policy)).
		Build()
	if err != nil {
		zap.S().Error("stream could not be built", zap.Errors("errors", err))
		return
	}

	// start the continuous query
	q.Run()
	// always close your query when no longer needed to free resources
	defer q.Close()

	res, err2 := pubsub.Subscribe[float64](q.Output.ID())
	if err2 != nil {
		zap.S().Error("could not subscribe", zap.Error(err2))
	}

	publishEvents(res)
	waitForProcessedEvents(res)
}

func waitForProcessedEvents(res *pubsub.StreamReceiver[float64]) {
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

func publishEvents(res *pubsub.StreamReceiver[float64]) {
	go func() {
		for i := 0; i < 100000; i++ {
			if err := pubsub.PublishN[float64]("in", events.NewEvent[float64](rand.Float64())); err != nil {
				zap.S().Error("publish error", zap.Error(err))
			}
		}
		// close channel
		pubsub.Unsubscribe(res)
	}()
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}
