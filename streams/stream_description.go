package streams

import (
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

func (d *StreamDescription) StreamID() StreamID {
	return StreamID(d.ID)
}

func DeleteStream(description *StreamDescription) {
	PubSubSystem.RemoveStream(description.StreamID())
}

func InstantiateStream(description *StreamDescription) {
	var stream Stream

	if description.Async {
		stream = NewLocalAsyncStream(description.Name, description.StreamID())
	} else {
		stream = NewLocalSyncStream(description.Name, description.StreamID())
	}

	PubSubSystem.NewOrReplaceStream(stream.ID(), stream)
}

func StreamDescriptionFromYML(b []byte) (*StreamDescription, error) {
	var d = &StreamDescription{}
	if err := yaml.Unmarshal(b, d); err != nil {
		return nil, err
	}

	if d.Name == "" {
		return nil, errors.New("no name provided")
	}

	ensureStreamIDIsProvided(d)

	return d, nil
}

func ensureStreamIDIsProvided(d *StreamDescription) {
	// if not provided, create a new ID
	if d.ID == uuid.Nil {
		d.ID = uuid.New()
	}
}
