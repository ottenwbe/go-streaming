package pubsub_test

import (
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/pkg/pubsub"
	"gopkg.in/yaml.v3"
	"reflect"
)

var (
	exampleJson = `{
  "topic": "my_json_topic",
  "type": "string"
}`
	exampleYml = `topic: my_yml_topic
type: int`
)

var _ = Describe("StreamID", func() {
	Describe("IsNil", func() {
		It("should return true for nilTopic", func() {
			id := pubsub.NilStreamID()
			Expect(id.IsNil()).To(BeTrue())
		})

		It("should return false for a valid string", func() {
			id := pubsub.MakeStreamID[int]("valid_stream_id")
			Expect(id.IsNil()).To(BeFalse())
		})

		It("should return true for an empty string", func() {
			id := pubsub.MakeStreamID[int]("")
			Expect(id.IsNil()).To(BeTrue())
		})
	})

	Describe("Make method", func() {
		It("should create a valid id", func() {
			const testTopic = "make-1"
			id := pubsub.MakeStreamID[int](testTopic)
			Expect(id.Topic).To(Equal(testTopic))
			Expect(id.TopicType.String()).To(Equal("int"))
		})
	})

	Describe("String method", func() {
		It("should return the underlying string", func() {
			id := pubsub.MakeStreamID[int]("test_id")
			Expect(id.String()).To(Equal("test_id"))
		})
	})

	Describe("NilStreamID function", func() {
		It("should return the nilTopic constant", func() {
			id := pubsub.NilStreamID()
			Expect(id).To(Equal(pubsub.NilStreamID()))
		})
	})

	Describe("RandomStreamID function", func() {
		It("should return a non-empty string", func() {
			id := pubsub.RandomStreamID()
			Expect(id).NotTo(Equal(pubsub.NilStreamID()))
			Expect(id).NotTo(Equal(""))
		})
	})

	Describe("Unmarshal functions", func() {
		It("should be able to unmarshal a json", func() {

			var id pubsub.StreamID
			json.Unmarshal([]byte(exampleJson), &id)

			Expect(id.Topic).To(Equal("my_json_topic"))
			Expect(id.TopicType).To(Equal(reflect.TypeOf("")))
		})
		It("should be able to unmarshal a yaml", func() {

			var id pubsub.StreamID
			yaml.Unmarshal([]byte(exampleYml), &id)

			Expect(id.Topic).To(Equal("my_yml_topic"))
			Expect(id.TopicType).To(Equal(reflect.TypeOf(1)))
		})
	})

	Describe("Marshal functions", func() {
		It("should be able to marshal a json", func() {
			id := pubsub.RandomStreamID()

			b, err := json.Marshal(id)

			Expect(string(b)).To(ContainSubstring(id.Topic))
			Expect(err).To(BeNil())
		})
		It("should be able to marshal a json with a correct type", func() {
			id := pubsub.MakeStreamID[int]("json-1")

			b, err := json.Marshal(id)

			Expect(string(b)).To(ContainSubstring("int"))
			Expect(err).To(BeNil())
		})
		It("should be able to marshal a yaml", func() {
			id := pubsub.RandomStreamID()

			b, err := yaml.Marshal(id)

			Expect(string(b)).To(ContainSubstring(id.Topic))
			Expect(err).To(BeNil())
		})
		It("should be able to marshal a yaml with a correct type", func() {
			id := pubsub.MakeStreamID[int]("json-1")

			b, err := yaml.Marshal(id)

			fmt.Println(string(b))

			Expect(string(b)).To(ContainSubstring("int"))
			Expect(err).To(BeNil())
		})
	})

})
