package query

import (
	"github.com/google/uuid"
	"go-stream-processing/internal/engine"
	"go-stream-processing/internal/pubsub"
)

type QueryControl struct {
	ID        ID
	Operators []engine.OperatorControl
	Streams   []pubsub.StreamControl
	Output    pubsub.StreamControl
}

func newQueryControl(outStream pubsub.StreamControl) *QueryControl {
	return &QueryControl{
		ID:        ID(uuid.New()),
		Operators: make([]engine.OperatorControl, 0),
		Streams:   []pubsub.StreamControl{outStream},
		Output:    outStream,
	}
}

func (c *QueryControl) addStreams(streams ...pubsub.StreamControl) {
	c.Streams = append(c.Streams, streams...)
}

func (c *QueryControl) addOperations(operators ...engine.OperatorControl) {
	c.Operators = append(c.Operators, operators...)
}

func (c *QueryControl) start() {
	for _, stream := range c.Streams {
		stream.Run()
	}
	for _, operator := range c.Operators {
		operator.Start()
	}
}

func (c *QueryControl) stop() {
	for _, operator := range c.Operators {
		operator.Stop()
	}
	for _, stream := range c.Streams {
		stream.TryClose()
	}
}
