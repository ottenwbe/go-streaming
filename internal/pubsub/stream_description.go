package pubsub

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// StreamDescription details the stream configurations
type StreamDescription struct {
	Name  string    `yaml:"name"`
	ID    uuid.UUID `yaml:"id"`
	Async bool      `yaml:"async"`
}

func NewStreamDescription(name string, id uuid.UUID, async bool) StreamDescription {
	return StreamDescription{
		Name:  name,
		ID:    id,
		Async: async,
	}
}

func (d StreamDescription) Equal(comp StreamDescription) bool {
	return d.Name == comp.Name && d.ID == comp.ID
}

func (d StreamDescription) StreamID() StreamID {
	return StreamID(d.ID)
}

func StreamDescriptionEnrichment(d StreamDescription) (StreamDescription, error) {

	if d.Name == "" {
		return StreamDescription{}, errors.New("no name provided")
	}

	ensureStreamIDIsProvided(&d)

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

func ensureStreamIDIsProvided(d *StreamDescription) {
	// if not provided, create a new ID
	if d.ID == uuid.Nil {
		d.ID = uuid.New()
	}
}
