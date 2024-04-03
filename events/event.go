package events

import (
	"encoding/json"
	"go.uber.org/zap"
	"time"
)

type Event[T any] interface {
	GetTimestamp() time.Time
	GetContent() T
}

func Arr[T any](events ...Event[T]) []Event[T] {
	return events
}

type EventChannel[T any] chan Event[T]

type TemporalEvent[T any] struct {
	TimeStamp time.Time
	Content   T
}

type OrchestrationEvent struct {
	TimeStamp time.Time
}

func (m *OrchestrationEvent) GetTimestamp() time.Time {
	return m.TimeStamp
}

func (m *OrchestrationEvent) GetContent(v interface{}) error {
	v = nil
	return nil
}

func NewEvent[T any](content T) Event[T] {
	return &TemporalEvent[T]{
		TimeStamp: time.Now(),
		Content:   content,
	}
}

func NewEventFromJSON(b []byte) (Event[map[string]interface{}], error) {
	content := make(map[string]interface{})
	err := json.Unmarshal(b, &content)
	if err != nil {
		zap.S().Error("error could not be unmarshalled", err)
		return nil, err
	}

	return &TemporalEvent[map[string]interface{}]{
		TimeStamp: time.Now(),
		Content:   content,
	}, nil
}

func (e *TemporalEvent[T]) GetTimestamp() time.Time {
	return e.TimeStamp
}

func (e *TemporalEvent[T]) GetContent() T {
	return e.Content
}
