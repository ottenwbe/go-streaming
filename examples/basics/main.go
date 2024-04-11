package main

import (
	"fmt"
	"go-stream-processing/internal/buffer"
	"go-stream-processing/internal/events"
	"go-stream-processing/internal/pubsub"
	"go-stream-processing/pkg/query"
	"go.uber.org/zap"
	"math/rand"
)

func main() {
	policy := buffer.NewCountingWindowPolicy[float64](10, 10)
	q1, err := query.ContinuousBatchSum[float64]("in", "out", policy)
	if err != nil {
		zap.S().Error("could not create query", zap.Error(err))
	}
	q2, err := query.ContinuousConvert[float64, int]("out", "fin")
	if err != nil {
		zap.S().Error("could not create query", zap.Error(err))
	}

	q1.And(q2).Run()
	defer q1.Close()

	res, err := pubsub.Subscribe[int](q2.Output.ID())
	if err != nil {
		zap.S().Error("could not subscribe", zap.Error(err))
	}

	var fin = make(chan bool)
	go func() {
		for i := 0; i < 100000; i++ {
			if err := pubsub.PublishN[float64]("in", events.NewEvent[float64](rand.Float64())); err != nil {
				fmt.Println(err)
			}
		}
		pubsub.Unsubscribe[int](res)
	}()

	go func() {
		for {
			if e, more := <-res.Notify; more {
				fmt.Println(e)
			} else {
				fin <- true
			}
		}
	}()

	<-fin
}
