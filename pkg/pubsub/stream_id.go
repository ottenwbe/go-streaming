package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"reflect"
)

const (
	topicKey = "topic"
	typeKey  = "type"
)

var (
	UnmarshallingTopicNotStringError  = errors.New("streamID: error unmarshalling topic; expected string")
	UnmarshallingTopicMissingKeyError = errors.New("streamID: error unmarshalling topic; missing key")
)

type StreamID struct {
	Topic     string
	TopicType reflect.Type
}

type marshalledStreamID struct {
	Topic     string `yaml:"topic"` // include topic as a string
	TopicType string `yaml:"type"`  // include topicType as a string
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
