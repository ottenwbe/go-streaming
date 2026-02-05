package pubsub

import (
	"encoding/json"
	"errors"

	"gopkg.in/yaml.v3"
)

var StreamDescriptionWithoutID = errors.New("stream description: no id provided")

// StreamDescription details the stream configurations
type StreamDescription struct {
	ID            StreamID `yaml:"id" json:"id"`
	AsyncStream   bool     `yaml:"asyncStream" json:"asyncStream"`
	AsyncReceiver bool     `yaml:"asyncReceiver" json:"asyncReceiver"`
}

type StreamOption func(*StreamDescription)

func WithAsyncStream(async bool) StreamOption {
	return func(s *StreamDescription) {
		s.AsyncStream = async
	}
}

func WithAsyncReceiver(asyncReceiver bool) StreamOption {
	return func(s *StreamDescription) {
		s.AsyncReceiver = asyncReceiver
	}
}

// MakeStreamDescription creates a new StreamDescription with the provided parameters.
func MakeStreamDescription[T any](topic string, options ...StreamOption) StreamDescription {
	return MakeStreamDescriptionFromID(MakeStreamID[T](topic), options...)
}

// MakeStreamDescriptionFromID creates a new StreamDescription using an existing StreamID.
func MakeStreamDescriptionFromID(id StreamID, options ...StreamOption) StreamDescription {
	d := StreamDescription{
		ID: id,
	}
	for _, option := range options {
		option(&d)
	}
	return d
}

// EqualTo checks if two StreamDescriptions are equal based on their ID.
func (d StreamDescription) EqualTo(comp StreamDescription) bool {
	return d.ID == comp.ID
}

// StreamID returns the ID associated with the StreamDescription.
func (d StreamDescription) StreamID() StreamID {
	return d.ID
}

// StreamDescriptionValidation validates the StreamDescription, ensuring it has a valid ID.
func StreamDescriptionValidation(d StreamDescription) (StreamDescription, error) {
	if d.ID.IsNil() {
		return StreamDescription{}, StreamDescriptionWithoutID
	}

	return d, nil
}

// StreamDescriptionFromJSON parses a StreamDescription from a JSON byte slice.
func StreamDescriptionFromJSON(b []byte) (StreamDescription, error) {
	var d = StreamDescription{}
	if err := json.Unmarshal(b, &d); err != nil {

		return StreamDescription{}, err
	}

	return StreamDescriptionValidation(d)
}

// StreamDescriptionFromYML parses a StreamDescription from a YAML byte slice.
func StreamDescriptionFromYML(b []byte) (StreamDescription, error) {
	var d = StreamDescription{}
	if err := yaml.Unmarshal(b, &d); err != nil {
		return StreamDescription{}, err
	}

	return StreamDescriptionValidation(d)
}
