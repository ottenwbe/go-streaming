package pubsub

import (
	"encoding/json"
	"errors"

	"github.com/goccy/go-yaml"
	"github.com/ottenwbe/go-streaming/pkg/events"
)

var (
	ErrStreamDescriptionWithoutID = errors.New("stream configuration: no id provided")
)

// SubscriberConfig details the subscriber configurations
type SubscriberConfig struct {
	Synchronous           bool                         `yaml:"synchronous" json:"synchronous"`
	BufferCapacity        int                          `yaml:"bufferCapacity" json:"bufferCapacity"`
	BufferPolicySelection events.SelectionPolicyConfig `yaml:"selectionPolicy" json:"selectionPolicy"`
}

// StreamConfig details the stream configurations
type StreamConfig struct {
	ID                 StreamID         `yaml:"id" json:"id"`
	Asynchronous       bool             `yaml:"asyncStream" json:"asyncStream"`
	BufferCapacity     int              `yaml:"bufferCapacity" json:"bufferCapacity"`
	AutoCleanup        bool             `yaml:"autoCleanup" json:"autoCleanup"`
	AutoStart          bool             `yaml:"autoStart" json:"autoStart"`
	DefaultSubscribers SubscriberConfig `yaml:"subscribers" json:"subscribers"`
	Sort               bool             `yaml:"sorted" json:"sorted"`
}

// SubscriberOption allows to configure the subscription / subscriber config
type SubscriberOption func(*SubscriberConfig)

// SubscriberWithSelectionPolicy allows to provide a selection policy for the subscriber
func SubscriberWithSelectionPolicy(p events.SelectionPolicyConfig) SubscriberOption {
	return func(s *SubscriberConfig) {
		s.BufferPolicySelection = p
	}
}

func SubscriberIsSync(synchronous bool) SubscriberOption {
	return func(s *SubscriberConfig) {
		s.Synchronous = synchronous
	}
}

func SubscriberWithBufferCapacity(capacity int) SubscriberOption {
	return func(s *SubscriberConfig) {
		s.BufferCapacity = capacity
	}
}

type StreamOption func(*StreamConfig)

func WithSubscriberSelectionPolicy(p events.SelectionPolicyConfig) StreamOption {
	return func(s *StreamConfig) {
		s.DefaultSubscribers.BufferPolicySelection = p
	}
}

func WithSubscriberSync(synchronous bool) StreamOption {
	return func(s *StreamConfig) {
		s.DefaultSubscribers.Synchronous = synchronous
	}
}

func WithSubscriberBufferCapacity(capacity int) StreamOption {
	return func(s *StreamConfig) {
		s.DefaultSubscribers.BufferCapacity = capacity
	}
}

func WithAsynchronousStream(async bool) StreamOption {
	return func(s *StreamConfig) {
		s.Asynchronous = async
	}
}

func WithSorted(sorted bool) StreamOption {
	return func(s *StreamConfig) {
		s.Sort = sorted
	}
}

func WithBufferCapacity(capacity int) StreamOption {
	return func(s *StreamConfig) {
		s.BufferCapacity = capacity
	}
}

func WithAutoCleanup(autoCleanup bool) StreamOption {
	return func(s *StreamConfig) {
		s.AutoCleanup = autoCleanup
	}
}

func WithAutoStart(autoStart bool) StreamOption {
	return func(s *StreamConfig) {
		s.AutoStart = autoStart
	}
}

func WithDefaultSubscribers(subscribers SubscriberConfig) StreamOption {
	return func(s *StreamConfig) {
		s.DefaultSubscribers = subscribers
	}
}

// MakeStreamConfig creates a new StreamConfig with the provided parameters.
func MakeStreamConfig[T any](topic string, options ...StreamOption) StreamConfig {
	return MakeStreamConfigByID(MakeStreamID[T](topic), options...)
}

// MakeStreamConfigByID creates a new StreamConfig using an existing StreamID.
func MakeStreamConfigByID(id StreamID, options ...StreamOption) StreamConfig {
	d := StreamConfig{
		ID:        id,
		AutoStart: true,
	}
	for _, option := range options {
		option(&d)
	}

	return d
}

func EnrichSubscriberConfig(description *SubscriberConfig, options ...SubscriberOption) {
	if description != nil {
		for _, option := range options {
			option(description)
		}
	}
}

// MakeSubscriberConfig creates a new SubscriberConfig based on provided options.
func MakeSubscriberConfig(options ...SubscriberOption) SubscriberConfig {
	d := SubscriberConfig{}
	for _, option := range options {
		option(&d)
	}
	return d
}

// EqualTo checks if two StreamDescriptions are equal based on their ID.
func (d StreamConfig) EqualTo(comp StreamConfig) bool {
	return d.ID == comp.ID
}

// StreamID returns the ID associated with the StreamConfig.
func (d StreamConfig) StreamID() StreamID {
	return d.ID
}

// StreamDescriptionValidation validates the StreamConfig, ensuring it has a valid ID.
func StreamDescriptionValidation(d StreamConfig) (StreamConfig, error) {
	if d.ID.IsNil() {
		return StreamConfig{}, ErrStreamDescriptionWithoutID
	}

	return d, nil
}

// StreamDescriptionFromJSON parses a StreamConfig from a JSON byte slice.
func StreamDescriptionFromJSON(b []byte) (StreamConfig, error) {
	var d = StreamConfig{
		AutoStart: true,
	}
	if err := json.Unmarshal(b, &d); err != nil {

		return StreamConfig{}, err
	}

	return StreamDescriptionValidation(d)
}

// StreamDescriptionFromYML parses a StreamConfig from a YAML byte slice.
func StreamDescriptionFromYML(b []byte) (StreamConfig, error) {
	var d = StreamConfig{
		AutoStart: true,
	}
	if err := yaml.Unmarshal(b, &d); err != nil {
		return StreamConfig{}, err
	}

	return StreamDescriptionValidation(d)
}
