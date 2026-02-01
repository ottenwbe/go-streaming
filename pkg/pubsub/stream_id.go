package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

const (
	topicKey = "topic"
	typeKey  = "type"
)

var (
	UnmarshallingTopicNotStringError  = errors.New("streamID: error unmarshalling topic; expected string")
	UnmarshallingTopicMissingKeyError = errors.New("streamID: error unmarshalling topic; missing key")
)

// StreamID uniquely identifies a stream by its topic and the type of data it carries.
type StreamID struct {
	Topic     string
	TopicType reflect.Type
}

type marshalledStreamID struct {
	Topic     string `yaml:"topic"` // include topic as a string
	TopicType string `yaml:"type"`  // include topicType as a string
}

// MarshalJSON implements the json.Marshaler interface for StreamID.
func (s StreamID) MarshalJSON() ([]byte, error) {
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

	if topic, ok := unmarshalled[topicKey]; ok {
		s.Topic, ok = topic.(string)
		if !ok {
			return UnmarshallingTopicNotStringError
		}
	} else {
		return UnmarshallingTopicMissingKeyError
	}

	if topicType, ok := unmarshalled[typeKey]; ok {
		s.TopicType, err = convertInterfaceToTopicType(topicType)
		if err != nil {
			return fmt.Errorf("error unmarshalling topicType: %w", err)
		}
	} else {
		s.TopicType = nil
	}
	return nil
}

// MarshalYAML implements the yaml.Marshaler interface for StreamID.
func (s StreamID) MarshalYAML() (interface{}, error) {
	data := s.marshalStreamID()
	return data, nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for StreamID.
func (s *StreamID) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var unmarshalled map[string]interface{}
	err := unmarshal(&unmarshalled)
	if err != nil {
		return err
	}

	if topic, ok := unmarshalled[topicKey]; ok {
		s.Topic, ok = topic.(string)
		if !ok {
			return UnmarshallingTopicNotStringError
		}
	} else {
		return UnmarshallingTopicMissingKeyError
	}

	if topicType, ok := unmarshalled[typeKey]; ok {
		s.TopicType, err = convertInterfaceToTopicType(topicType)
		if err != nil {
			return fmt.Errorf("error unmarshalling topicType: %w", err)
		}
	} else {
		s.TopicType = nil
	}
	return nil
}

var typeMap = map[string]reflect.Type{
	"int":                     reflect.TypeOf(int(0)),
	"int32":                   reflect.TypeOf(int32(0)),
	"int64":                   reflect.TypeOf(int64(0)),
	"float32":                 reflect.TypeOf(float32(0)),
	"float64":                 reflect.TypeOf(float64(0)),
	"string":                  reflect.TypeOf(""),
	"map[string]interface{}":  reflect.TypeOf(map[string]interface{}{}),
	"map[string]interface {}": reflect.TypeOf(map[string]interface{}{}),
}

func getTypeFromString(typeName string) reflect.Type {
	if t, ok := typeMap[typeName]; ok {
		return t
	}
	return nil
}

func convertInterfaceToTopicType(i interface{}) (reflect.Type, error) {
	typeName, ok := i.(string)
	if !ok {
		return nil, fmt.Errorf("expected string type for topicType, got %T", i)
	}
	return getTypeFromString(typeName), nil
}

func (s StreamID) marshalStreamID() marshalledStreamID {

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
func (s StreamID) IsNil() bool {
	return s.Topic == nilTopic
}

// String returns the topic string of the StreamID.
func (s StreamID) String() string {
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
	var tmp T
	return StreamID{
		Topic:     topic,
		TopicType: reflect.TypeOf(tmp),
	}
}
