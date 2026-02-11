package pubsub

import (
	"encoding/json"
	"errors"

	"github.com/ottenwbe/go-streaming/pkg/selection"
	"gopkg.in/yaml.v3"
)

var (
	StreamDescriptionWithoutID = errors.New("stream description: no id provided")
)

// SubscriberDescription details the subscriber configurations
type SubscriberDescription struct {
	Synchronous           bool                        `yaml:"synchronous" json:"synchronous"`
	BufferCapacity        int                         `yaml:"bufferCapacity" json:"bufferCapacity"`
	BufferPolicySelection selection.PolicyDescription `yaml:"selectionPolicy" json:"selectionPolicy"`
}

// StreamDescription details the stream configurations
type StreamDescription struct {
	ID                 StreamID              `yaml:"id" json:"id"`
	Asynchronous       bool                  `yaml:"asyncStream" json:"asyncStream"`
	BufferCapacity     int                   `yaml:"bufferCapacity" json:"bufferCapacity"`
	AutoCleanup        bool                  `yaml:"autoCleanup" json:"autoCleanup"`
	DefaultSubscribers SubscriberDescription `yaml:"subscribers" json:"subscribers"`
}

// SubscriberOption allows to configure the subscription
type SubscriberOption func(*SubscriberDescription)

// SubscriberWithSelectionPolicy allows to provide a selection policy for the subscriber
func SubscriberWithSelectionPolicy(p selection.PolicyDescription) SubscriberOption {
	return func(s *SubscriberDescription) {
		s.BufferPolicySelection = p
	}
}

func SubscriberIsSync(synchronous bool) SubscriberOption {
	return func(s *SubscriberDescription) {
		s.Synchronous = synchronous
	}
}

func SubscriberWithBufferCapacity(capacity int) SubscriberOption {
	return func(s *SubscriberDescription) {
		s.BufferCapacity = capacity
	}
}

type StreamOption func(*StreamDescription)

func WithSubscriberSelectionPolicy(p selection.PolicyDescription) StreamOption {
	return func(s *StreamDescription) {
		s.DefaultSubscribers.BufferPolicySelection = p
	}
}

func WithSubscriberSync(synchronous bool) StreamOption {
	return func(s *StreamDescription) {
		s.DefaultSubscribers.Synchronous = synchronous
	}
}

func WithSubscriberBufferCapacity(capacity int) StreamOption {
	return func(s *StreamDescription) {
		s.DefaultSubscribers.BufferCapacity = capacity
	}
}

func WithAsynchronousStream(async bool) StreamOption {
	return func(s *StreamDescription) {
		s.Asynchronous = async
	}
}

func WithBufferCapacity(capacity int) StreamOption {
	return func(s *StreamDescription) {
		s.BufferCapacity = capacity
	}
}

func WithAutoCleanup(autoCleanup bool) StreamOption {
	return func(s *StreamDescription) {
		s.AutoCleanup = autoCleanup
	}
}

func WithDefaultSubscribers(subscribers SubscriberDescription) StreamOption {
	return func(s *StreamDescription) {
		s.DefaultSubscribers = subscribers
	}
}

// MakeStreamDescription creates a new StreamDescription with the provided parameters.
func MakeStreamDescription[T any](topic string, options ...StreamOption) StreamDescription {
	return MakeStreamDescriptionByID(MakeStreamID[T](topic), options...)
}

// MakeStreamDescriptionByID creates a new StreamDescription using an existing StreamID.
func MakeStreamDescriptionByID(id StreamID, options ...StreamOption) StreamDescription {
	d := StreamDescription{
		ID: id,
	}
	for _, option := range options {
		option(&d)
	}

	return d
}

func EnrichSubscriberDescription(description *SubscriberDescription, options ...SubscriberOption) {
	if description != nil {
		for _, option := range options {
			option(description)
		}
	}
}

// MakeSubscriberDescription creates a new SubscriberDescription based on provided options.
func MakeSubscriberDescription(options ...SubscriberOption) SubscriberDescription {
	d := SubscriberDescription{}
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
