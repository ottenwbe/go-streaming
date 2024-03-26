package events

import (
	"encoding/json"
	"time"
)

type Event interface {
	GetTimestamp() time.Time
	GetContent(v interface{}) error
}

type EventChannel chan Event

type SimpleEvent struct {
	timeStamp time.Time
	Content   []byte
}

type Marker struct {
	timeStamp time.Time
}

func (m *Marker) GetTimestamp() time.Time {
	//TODO implement me
	panic("implement me")
}

func (m *Marker) GetContent(v interface{}) error {
	//TODO implement me
	panic("implement me")
}

func NewSimpleEvent(content []byte) Event {
	return &SimpleEvent{
		timeStamp: time.Now(),
		Content:   content,
	}
}

func NewEvent(content interface{}) (Event, error) {

	contentBytes, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}

	return NewSimpleEvent(contentBytes), nil
}

func (e *SimpleEvent) GetTimestamp() time.Time {
	return e.timeStamp
}

func (e *SimpleEvent) GetContent(v interface{}) error {
	return json.Unmarshal(e.Content, v)
}
