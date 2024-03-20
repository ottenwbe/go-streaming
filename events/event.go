package events

import (
	"encoding/json"
	"time"
)

type Event interface {
	GetTimestamp() time.Time
	GetContent(v interface{}) error
}

type SimpleEvent struct {
	timeStamp time.Time
	Content   []byte
}

func NewEventB(content []byte) Event {
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

	return NewEventB(contentBytes), nil
}

func (e *SimpleEvent) GetTimestamp() time.Time {
	return e.timeStamp
}

func (e *SimpleEvent) GetContent(v interface{}) error {
	return json.Unmarshal(e.Content, v)
}
