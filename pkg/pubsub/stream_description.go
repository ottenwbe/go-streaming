package pubsub

import (
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v3"
)

var StreamDescriptionWithoutID = errors.New("stream description: no id provided")

// StreamDescription details the stream configurations
type StreamDescription struct {
	ID          StreamID `yaml,json:"id"`
	Async       bool     `yaml,json:"async"`
	SingleFanIn bool     `yaml,json:"singleFanIn"`
}

func MakeStreamDescription[T any](topic string, async bool, singleFanIn bool) StreamDescription {
	return StreamDescription{
		ID:          MakeStreamID[T](topic),
		Async:       async,
		SingleFanIn: singleFanIn,
	}
}

func MakeStreamDescriptionFromID(id StreamID, async bool, singleFanIn bool) StreamDescription {
	return StreamDescription{
		ID:          id,
		Async:       async,
		SingleFanIn: singleFanIn,
	}
}

func (d StreamDescription) EqualTo(comp StreamDescription) bool {
	return d.ID == comp.ID
}

func (d StreamDescription) StreamID() StreamID {
	return d.ID
}

func StreamDescriptionValidation(d StreamDescription) (StreamDescription, error) {
	if d.ID.IsNil() {
		return StreamDescription{}, StreamDescriptionWithoutID
	}

	return d, nil
}

func StreamDescriptionFromJSON(b []byte) (StreamDescription, error) {
	var d = StreamDescription{}
	if err := json.Unmarshal(b, &d); err != nil {

		return StreamDescription{}, err
	}

	return StreamDescriptionValidation(d)
}

func StreamDescriptionFromYML(b []byte) (StreamDescription, error) {
	var d = StreamDescription{}
	if err := yaml.Unmarshal(b, &d); err != nil {
		return StreamDescription{}, err
	}

	return StreamDescriptionValidation(d)
}
