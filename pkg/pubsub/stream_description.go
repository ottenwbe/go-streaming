package pubsub

import (
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v3"
)

// StreamDescription details the stream configurations
type StreamDescription struct {
	ID    StreamID `yaml:"id"`
	Async bool     `yaml:"async"`
}

func NewStreamDescription(id StreamID, async bool) StreamDescription {
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

	if d.ID == "" {
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
