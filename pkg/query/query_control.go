package query

import (
	"github.com/google/uuid"
	"go-stream-processing/internal/engine"
	pubsub2 "go-stream-processing/pkg/pubsub"
)

type ContinuousQuery struct {
	id        ID
	operators []engine.OperatorControl
	streams   []pubsub2.StreamControl
	Output    pubsub2.StreamControl
}

func (c *ContinuousQuery) With(c2 *ContinuousQuery) *ContinuousQuery {

	if c2.Output != nil && in(c.streams, c2.Output.ID()) {
		c2.Output = c.Output
	} else if (c.Output != nil && in(c2.streams, c.Output.ID())) || (c.Output == nil && c2.Output != nil) {
		c.Output = c2.Output
	} else {
		panic("output streams don't match")
	}

	c.addStreams(c2.streams...)
	c.addOperations(c2.operators...)

	return c
}

func (c *ContinuousQuery) ID() ID {
	return c.id
}

func (c *ContinuousQuery) Close() {
	c.stopEverything()

	pubsub2.TryRemoveStreams(c.streams)
	engine.OperatorRepository().Remove(c.operators)

	QueryRepository().remove(c.id)
}

func (c *ContinuousQuery) Run() {
	c.startEverything()
}

func newQueryControl(outStream pubsub2.StreamControl) *ContinuousQuery {
	return &ContinuousQuery{
		id:        ID(uuid.New()),
		operators: make([]engine.OperatorControl, 0),
		streams:   []pubsub2.StreamControl{outStream},
		Output:    outStream,
	}
}

func (c *ContinuousQuery) addStreams(streams ...pubsub2.StreamControl) {
	c.streams = append(c.streams, streams...)
}

func (c *ContinuousQuery) addOperations(operators ...engine.OperatorControl) {
	c.operators = append(c.operators, operators...)
}

func (c *ContinuousQuery) startEverything() {
	for _, stream := range c.streams {
		stream.Run()
	}
	for _, operator := range c.operators {
		operator.Start()
	}
}

func (c *ContinuousQuery) stopEverything() {
	for _, operator := range c.operators {
		operator.Stop()
	}
	for _, stream := range c.streams {
		stream.TryClose()
	}
}

func in(streams []pubsub2.StreamControl, id pubsub2.StreamID) bool {
	for _, stream := range streams {
		if stream.ID() == id {
			return true
		}
	}
	return false
}
