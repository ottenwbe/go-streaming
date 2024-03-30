package events

import (
	"encoding/json"
	"go.uber.org/zap"
	"time"
)

type Event interface {
	GetTimestamp() time.Time
	GetContentMap() map[string]interface{}
	GetContent(key string) interface{}
}

type EventChannel chan Event

type TemporalEvent struct {
	timeStamp time.Time
	content   map[string]interface{}
}

type OrchestrationEvent struct {
	timeStamp time.Time
}

func (m *OrchestrationEvent) GetTimestamp() time.Time {
	return m.timeStamp
}

func (m *OrchestrationEvent) GetContent(v interface{}) error {
	v = nil
	return nil
}

func NewEventMap(content map[string]interface{}) Event {
	return &TemporalEvent{
		timeStamp: time.Now(),
		content:   content,
	}
}

func NewEvent(key string, value interface{}) Event {
	return &TemporalEvent{
		timeStamp: time.Now(),
		content:   map[string]interface{}{key: value},
	}
}

func NewEventFromJSON(b []byte) (Event, error) {
	content := make(map[string]interface{})
	err := json.Unmarshal(b, &content)
	if err != nil {
		zap.S().Error("error could not be unmarshalled", err)
		return nil, err
	}

	return &TemporalEvent{
		timeStamp: time.Now(),
		content:   content,
	}, nil
}

func (e *TemporalEvent) GetTimestamp() time.Time {
	return e.timeStamp
}

func (e *TemporalEvent) GetContentMap() map[string]interface{} {
	return e.content
}

func (e *TemporalEvent) GetContent(key string) interface{} {
	return e.content[key]
}
