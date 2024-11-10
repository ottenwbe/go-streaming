package events

import (
	"encoding/json"
)

type (
	// Event interface for arbitrary events with any content of type T
	Event[TContent any] interface {
		GetStamp() TimeStamp
		GetContent() TContent
	}
	// TemporalEvent is an event with a TimeStamp, which allows to record the start and end time of an event
	TemporalEvent[TContent any] struct {
		Stamp   TimeStamp
		Content TContent
	}
	EventChannel[TContent any] chan Event[TContent]
)

type (
	// NumericConstraint constraint to limit the type parameter to numeric types
	NumericConstraint interface {
		~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
	}
	// NumericEvent restricts the content to numeric data types
	NumericEvent[T NumericConstraint] struct {
		TemporalEvent[T]
	}
)

func NewNumericEvent[T NumericConstraint](content T) Event[T] {
	return &NumericEvent[T]{
		TemporalEvent: *NewEvent[T](content).(*TemporalEvent[T]),
	}
}

func NewEvent[TContent any](content TContent) Event[TContent] {
	return newStampedEvent[TContent](content, nil)
}

func NewEventFromOthers[TContent any](content TContent, others ...Event[TContent]) Event[TContent] {
	return newStampedEventFromOthers[TContent](content, nil, others...)
}

func NewEventM[TContent any](content TContent, meta StampMeta) Event[TContent] {
	return newStampedEvent[TContent](content, meta)
}

func NewEventFromOthersM[TContent any](content TContent, meta StampMeta, others ...Event[TContent]) Event[TContent] {
	return newStampedEventFromOthers[TContent](content, meta, others...)
}

func NewEventFromJSON(b []byte) (Event[map[string]interface{}], error) {
	content := make(map[string]interface{})
	err := json.Unmarshal(b, &content)
	if err != nil {
		return nil, err
	}

	return newStampedEvent[map[string]interface{}](content, nil), err
}

func newStampedEvent[TContent any](
	content TContent,
	meta StampMeta,
) *TemporalEvent[TContent] {

	return &TemporalEvent[TContent]{
		Stamp:   createStamp(meta),
		Content: content,
	}
}

func newStampedEventFromOthers[TContent any](
	content TContent,
	meta StampMeta,
	others ...Event[TContent],
) *TemporalEvent[TContent] {

	otherStamps := make([]TimeStamp, 0)
	for _, other := range others {
		otherStamps = append(otherStamps, other.GetStamp())
	}

	return &TemporalEvent[TContent]{
		Stamp:   createStampBasedOnOthers(meta, otherStamps...),
		Content: content,
	}
}

func (e *TemporalEvent[TContent]) GetStamp() TimeStamp {
	return e.Stamp
}

func (e *TemporalEvent[TContent]) GetContent() TContent {
	return e.Content
}

func Arr[TContent any](events ...Event[TContent]) []Event[TContent] {
	return events
}
