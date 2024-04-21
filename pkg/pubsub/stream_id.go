package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"reflect"
)

type StreamID struct {
	Topic     string
	TopicType reflect.Type
}

type marshalledStreamID struct {
	Topic     string `yaml:"topic"`
	TopicType string `yaml:"type"` // include topicType as a string
}

func (s StreamID) MarshalJSON() ([]byte, error) {
	data := s.marshalStreamID()
	return json.Marshal(data)
}

func (s *StreamID) UnmarshalJSON(data []byte) error {
	var unmarshalled map[string]interface{}
	err := json.Unmarshal(data, &unmarshalled)
	if err != nil {
		return err
	}

	if topic, ok := unmarshalled["topic"]; ok {
		s.Topic, ok = topic.(string)
		if !ok {
			return errors.New("error unmarshalling topic: expected string")
		}
	} else {
		return errors.New("error unmarshalling topic: missing key")
	}

	if topicType, ok := unmarshalled["type"]; ok {
		s.TopicType, err = convertInterfaceToType(topicType)
		if err != nil {
			return fmt.Errorf("error unmarshalling topicType: %w", err)
		}
	} else {
		s.TopicType = nil
	}
	return nil
}

func (s StreamID) MarshalYAML() (interface{}, error) {
	data := s.marshalStreamID()
	return data, nil
}

func (s *StreamID) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var unmarshalled map[string]interface{}
	err := unmarshal(&unmarshalled)
	if err != nil {
		return err
	}

	if topic, ok := unmarshalled["topic"]; ok {
		s.Topic, ok = topic.(string)
		if !ok {
			return fmt.Errorf("error unmarshalling topic: expected string")
		}
	} else {
		return errors.New("error unmarshalling topic: missing key")
	}

	if topicType, ok := unmarshalled["type"]; ok {
		s.TopicType, err = convertInterfaceToType(topicType)
		if err != nil {
			return fmt.Errorf("error unmarshalling topicType: %w", err)
		}
	} else {
		s.TopicType = nil
	}
	return nil
}

var typeMap = map[string]reflect.Type{
	"int":                    reflect.TypeOf(int(0)),
	"string":                 reflect.TypeOf(""),
	"map[string]interface{}": reflect.TypeOf(map[string]interface{}{}),
	// Add more mappings for other types as needed
}

func GetTypeFromString(typeName string) reflect.Type {
	if t, ok := typeMap[typeName]; ok {
		return t
	}
	return nil
}

func convertInterfaceToType(i interface{}) (reflect.Type, error) {
	typeName, ok := i.(string)
	if !ok {
		return nil, fmt.Errorf("expected string type for topicType, got %T", i)
	}
	return GetTypeFromString(typeName), nil
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

func (s StreamID) IsNil() bool {
	return s.Topic == nilTopic
}

func (s StreamID) String() string {
	return s.Topic
}

func NilStreamID() StreamID {
	return StreamID{
		Topic:     nilTopic,
		TopicType: reflect.TypeOf(nilTopic),
	}
}

func RandomStreamID() StreamID {
	return MakeStreamID[any](uuid.New().String())
}

func MakeStreamID[T any](topic string) StreamID {
	var tmp T
	return StreamID{
		Topic:     topic,
		TopicType: reflect.TypeOf(tmp),
	}
}
