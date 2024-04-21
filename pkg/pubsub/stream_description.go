package pubsub

import (
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v3"
)

// StreamDescription details the stream configurations
type StreamDescription struct {
	ID    StreamID `yaml,json:"id"`
	Async bool     `yaml,json:"async"`
}

func MakeStreamDescriptionByID(id StreamID, async bool) StreamDescription {
	return StreamDescription{
		ID:    id,
		Async: async,
	}
}

func MakeStreamDescriptionTypeless(topic string, async bool) StreamDescription {
	return StreamDescription{
		ID:    MakeStreamID[any](topic),
		Async: async,
	}
}

func MakeStreamDescription[T any](topic string, async bool) StreamDescription {
	return StreamDescription{
		ID:    MakeStreamID[T](topic),
		Async: async,
	}
}

func MakeStreamDescriptionID(id StreamID, async bool) StreamDescription {
	return StreamDescription{
		ID:    id,
		Async: async,
	}
}

func (d StreamDescription) Equal(comp StreamDescription) bool {
	return d.ID == comp.ID
}

func (d StreamDescription) StreamID() StreamID {
	return StreamID(d.ID)
}

func StreamDescriptionEnrichment(d StreamDescription) (StreamDescription, error) {

	if d.ID.IsNil() {
		return StreamDescription{}, errors.New("no id provided")
	}

	return d, nil
}

func StreamDescriptionFromJSON(b []byte) (StreamDescription, error) {
	var d = StreamDescription{}
	if err := json.Unmarshal(b, &d); err != nil {

		return StreamDescription{}, err
	}

	return StreamDescriptionEnrichment(d)
}

func StreamDescriptionFromYML(b []byte) (StreamDescription, error) {
	var d = StreamDescription{}
	if err := yaml.Unmarshal(b, &d); err != nil {
		return StreamDescription{}, err
	}

	return StreamDescriptionEnrichment(d)
}
