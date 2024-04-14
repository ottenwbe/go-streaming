package pubsub

import "github.com/google/uuid"

type StreamID string

const nilID = ""

func (s StreamID) IsNil() bool {
	return s == nilID
}

func (s StreamID) String() string {
	return string(s)
}

func NilStreamID() StreamID {
	return nilID
}

func RandomStreamID() StreamID {
	return StreamID(uuid.New().String())
}
