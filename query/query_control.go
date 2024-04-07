package query

import (
	"go-stream-processing/engine"
	"go-stream-processing/streams"
)

type QueryControl struct {
	ID        QueryID
	Operators []engine.OperatorControl
	Streams   []streams.StreamControl
}
