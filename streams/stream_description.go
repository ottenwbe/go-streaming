package api

import (
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type GraphDescription struct {
	Streams StreamDescription `yaml:"streams"`
}

type StreamDescription struct {
	Name  string    `yaml:"name"`
	ID    uuid.UUID `yaml:"id"`
	Async bool      `yaml:"async"`
}

func StreamDescriptionFromYML(b []byte) (*StreamDescription, error) {
	var d StreamDescription
	if err := yaml.Unmarshal(b, &d); err != nil {
		return nil, err
	}

	// if not provided, create a new ID
	if d.ID == uuid.Nil {
		d.ID = uuid.New()
	}

	return &d, nil
}
