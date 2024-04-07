package query

import (
	"github.com/google/uuid"
	"go-stream-processing/engine"
	"go-stream-processing/streams"
)

type QueryControl struct {
	ID        QueryID
	Operators []engine.OperatorControl
	Streams   []streams.StreamControl
}

func newQueryControl() *QueryControl {
	return &QueryControl{
		ID:        QueryID(uuid.New()),
		Operators: make([]engine.OperatorControl, 0),
		Streams:   make([]streams.StreamControl, 0),
	}
}

func (c *QueryControl) addStreams(streams ...streams.StreamControl) {
	c.Streams = append(c.Streams, streams...)
}

func (c *QueryControl) addOperations(operators ...engine.OperatorControl) {
	c.Operators = append(c.Operators, operators...)
}

func (c *QueryControl) start() {
	for _, stream := range c.Streams {
		stream.Start()
	}
	for _, operator := range c.Operators {
		operator.Start()
	}
}

func (c *QueryControl) stop() {
	for _, stream := range c.Streams {
		stream.Stop()
	}
	for _, operator := range c.Operators {
		operator.Stop()
	}
}
