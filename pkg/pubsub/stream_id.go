package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const (
	topicKey = "topic"
	typeKey  = "type"
)

var (
	ErrUnmarshallingTopicNotString  = errors.New("streamID: error unmarshalling topic; expected string")
	ErrUnmarshallingTopicMissingKey = errors.New("streamID: error unmarshalling topic; missing key")
	ErrUnmarshallingTypeNotString   = errors.New("streamID: error unmarshalling type; expected string")
	ErrUnknownType                  = errors.New("streamID: unknown type")
)

// StreamID uniquely identifies a stream by its topic and the type of data it carries.
type StreamID struct {
	Topic     string
	TopicType reflect.Type
}

type marshalledStreamID struct {
	Topic     string `yaml:"topic" json:"topic"` // include topic as a string
	TopicType string `yaml:"type" json:"type"`   // include topicType as a string
}

// MarshalJSON implements the json.Marshaler interface for StreamID.
func (s *StreamID) MarshalJSON() ([]byte, error) {
	data := s.marshalStreamID()
	return json.Marshal(data)
}

// UnmarshalJSON implements the json.Unmarshaler interface for StreamID.
func (s *StreamID) UnmarshalJSON(data []byte) error {
	var unmarshalled map[string]interface{}
	err := json.Unmarshal(data, &unmarshalled)
	if err != nil {
		return err
	}
	return s.unmarshalFromMap(unmarshalled)
}

// MarshalYAML implements the yaml.Marshaler interface for StreamID.
func (s *StreamID) MarshalYAML() (interface{}, error) {
	data := s.marshalStreamID()
	return data, nil
}

var (
	typeRegistryMutex sync.RWMutex
	typeRegistry      = map[string]reflect.Type{}
)

func RegisterType[T any]() {
	typeRegistryMutex.Lock()
	defer typeRegistryMutex.Unlock()

	t := reflect.TypeFor[T]()
	typeRegistry[t.String()] = t
}

func UnRegisterType[T any]() {
	typeRegistryMutex.Lock()
	defer typeRegistryMutex.Unlock()

	t := reflect.TypeFor[T]()
	delete(typeRegistry, t.String())
}

func getTypeFromString(typeName string) (reflect.Type, error) {
	typeRegistryMutex.RLock()
	defer typeRegistryMutex.RUnlock()

	if t, ok := typeRegistry[typeName]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrUnknownType, typeName)
}

func convertInterfaceToTopicType(i interface{}) (reflect.Type, error) {
	typeName, ok := i.(string)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", ErrUnmarshallingTypeNotString, i)
	}
	return getTypeFromString(typeName)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for StreamID.
func (s *StreamID) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var unmarshalled map[string]interface{}
	if err := unmarshal(&unmarshalled); err != nil {
		return err
	}
	return s.unmarshalFromMap(unmarshalled)
}

// StreamIDFromJSON parses a StreamID from a JSON byte slice.
func StreamIDFromJSON(b []byte) (StreamID, error) {
	var s StreamID
	if err := json.Unmarshal(b, &s); err != nil {
		return StreamID{}, err
	}
	return s, nil
}

// ToJSON converts a StreamID to its JSON representation.
func (s *StreamID) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

// StreamIDFromYML parses a StreamID from a YAML byte slice.
func StreamIDFromYML(b []byte) (StreamID, error) {
	var s StreamID
	if err := yaml.Unmarshal(b, &s); err != nil {
		return StreamID{}, err
	}
	return s, nil
}

// ToYML converts a StreamID to its YAML representation.
func (s *StreamID) ToYML() ([]byte, error) {
	return yaml.Marshal(s)
}

func (s *StreamID) unmarshalFromMap(data map[string]interface{}) error {
	var ok bool
	if topic, found := data[topicKey]; found {
		s.Topic, ok = topic.(string)
		if !ok {
			return ErrUnmarshallingTopicNotString
		}
	} else {
		return ErrUnmarshallingTopicMissingKey
	}

	if topicType, found := data[typeKey]; found {
		var err error
		s.TopicType, err = convertInterfaceToTopicType(topicType)
		return err
	}
	return nil
}

func (s *StreamID) marshalStreamID() marshalledStreamID {

	var t = "nil"
	if s.TopicType != nil {
		t = s.TopicType.String()
	}

	data := marshalledStreamID{
		Topic:     s.Topic,
		TopicType: t,
	}
	return data
}

const nilTopic = ""

// IsNil checks if the StreamID is the zero value (nil topic).
func (s *StreamID) IsNil() bool {
	if s == nil {
		return true
	}
	return s.Topic == nilTopic
}

// String returns the topic string of the StreamID.
func (s *StreamID) String() string {
	return s.Topic
}

// NilStreamID returns a StreamID representing a nil or empty stream.
func NilStreamID() StreamID {
	return StreamID{
		Topic:     nilTopic,
		TopicType: reflect.TypeOf(nilTopic),
	}
}

// RandomStreamID generates a new StreamID with a random UUID as the topic.
func RandomStreamID() StreamID {
	return MakeStreamID[any](uuid.New().String())
}

// MakeStreamID creates a StreamID for a given topic and generic type T.
func MakeStreamID[T any](topic string) StreamID {
	return StreamID{
		Topic:     topic,
		TopicType: reflect.TypeFor[T](),
	}
}
