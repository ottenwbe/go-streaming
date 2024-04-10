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

// number constraint to limit the type parameter to numeric types
type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

type NumericEvent[T number] struct {
	TemporalEvent[T]
}

func NewEvent[T any](content T) Event[T] {
	return &TemporalEvent[T]{
		TimeStamp: time.Now(),
		Content:   content,
	}
}

func NewNumericEvent[T number](content T) Event[T] {
	return &NumericEvent[T]{
		TemporalEvent[T]{
			TimeStamp: time.Now(),
			Content:   content,
		},
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
