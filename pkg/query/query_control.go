package query

import (
	"github.com/google/uuid"
	"go-stream-processing/internal/engine"
	"go-stream-processing/internal/pubsub"
)

type QueryControl struct {
	id        ID
	operators []engine.OperatorControl
	streams   []pubsub.StreamControl
	Output    pubsub.StreamControl
}

func (c *QueryControl) And(c2 *QueryControl) *QueryControl {

	if in(c.streams, c2.Output.ID()) {
		c2.Output = c.Output
	} else if in(c2.streams, c.Output.ID()) {
		c.Output = c2.Output
	} else {
		panic("output streams don't match")
	}

	c.addStreams(c2.streams...)
	c.addOperations(c2.operators...)

	return c
}

func (c *QueryControl) ID() ID {
	return c.id
}

func (c *QueryControl) Close() {
	c.stopEverything()

	pubsub.TryRemoveStreams(c.streams)
	engine.OperatorRepository().Remove(c.operators)

	QueryRepository().remove(c.id)
}

func (c *QueryControl) Run() {
	c.startEverything()
}

func newQueryControl(outStream pubsub.StreamControl) *QueryControl {
	return &QueryControl{
		id:        ID(uuid.New()),
		operators: make([]engine.OperatorControl, 0),
		streams:   []pubsub.StreamControl{outStream},
		Output:    outStream,
	}
}

func (c *QueryControl) addStreams(streams ...pubsub.StreamControl) {
	c.streams = append(c.streams, streams...)
}

func (c *QueryControl) addOperations(operators ...engine.OperatorControl) {
	c.operators = append(c.operators, operators...)
}

func (c *QueryControl) startEverything() {
	for _, stream := range c.streams {
		stream.Run()
	}
	for _, operator := range c.operators {
		operator.Start()
	}
}

func (c *QueryControl) stopEverything() {
	for _, operator := range c.operators {
		operator.Stop()
	}
	for _, stream := range c.streams {
		stream.TryClose()
	}
}

func in(streams []pubsub.StreamControl, id pubsub.StreamID) bool {
	for _, stream := range streams {
		if stream.ID() == id {
			return true
		}
	}
	return false
}
