package pubsub_test

import (
	"encoding/json"
	"reflect"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"gopkg.in/yaml.v3"
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

	BeforeEach(func() {
		pubsub.RegisterType[int]()
		pubsub.RegisterType[string]()
		pubsub.RegisterType[float64]()
		pubsub.RegisterType[float32]()
	})

	AfterEach(func() {
		pubsub.UnRegisterType[int]()
		pubsub.UnRegisterType[string]()
		pubsub.UnRegisterType[float64]()
		pubsub.UnRegisterType[float32]()
	})

	Describe("IsNil", func() {
		It("should return true for nil topic", func() {
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

	Describe("Make Ids", func() {
		It("should create a valid id", func() {
			const testTopic = "make-1"
			id := pubsub.MakeStreamID[int](testTopic)
			Expect(id.Topic).To(Equal(testTopic))
			Expect(id.TopicType.String()).To(Equal("int"))
		})
		It("should be be possible with a random id", func() {
			id := pubsub.RandomStreamID()
			Expect(len(id.Topic)).To(Equal(len(uuid.New().String())))
			Expect(id.TopicType).To(Equal(reflect.TypeFor[any]()))
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
		It("should fail for missing contents (json)", func() {
			var id pubsub.StreamID
			err := json.Unmarshal([]byte(""), &id)
			Expect(err).ToNot(BeNil())
		})
		It("should fail for wrong input (yml)", func() {
			var id pubsub.StreamID
			err := yaml.Unmarshal([]byte("{}"), &id)
			Expect(err).ToNot(BeNil())
		})
		It("should fail for missing topic key (json)", func() {
			var id pubsub.StreamID
			err := json.Unmarshal([]byte(`{"type":"int"}`), &id)
			Expect(err).To(Equal(pubsub.UnmarshallingTopicMissingKeyError))
		})
		It("should fail for wrong topic type (json)", func() {
			var id pubsub.StreamID
			err := json.Unmarshal([]byte(`{"topic":1,"type":"int"}`), &id)
			Expect(err).To(Equal(pubsub.UnmarshallingTopicNotStringError))
		})
		It("should fail for missing topic key (yml)", func() {
			var id pubsub.StreamID
			err := yaml.Unmarshal([]byte(`type: int`), &id)
			Expect(err).To(Equal(pubsub.UnmarshallingTopicMissingKeyError))
		})
		It("should fail for wrong topic type (yml)", func() {
			var id pubsub.StreamID
			err := yaml.Unmarshal([]byte(`topic: 1`), &id)
			Expect(err).To(Equal(pubsub.UnmarshallingTopicNotStringError))
		})

		It("should be able to unmarshal a yaml", func() {

			var id pubsub.StreamID
			yaml.Unmarshal([]byte(exampleYml), &id)

			Expect(id.Topic).To(Equal("my_yml_topic"))
			Expect(id.TopicType).To(Equal(reflect.TypeOf(1)))
		})

		It("should be able to unmarshal a with a float32 type", func() {
			var id pubsub.StreamID
			json.Unmarshal([]byte(`{"topic":"f32","type":"float32"}`), &id)
			Expect(id.TopicType).To(Equal(reflect.TypeOf(float32(1))))
		})
		It("should be able to unmarshal a with a float64 type", func() {
			var id pubsub.StreamID
			json.Unmarshal([]byte(`{"topic":"f32","type":"float64"}`), &id)
			Expect(id.TopicType).To(Equal(reflect.TypeOf(float64(1))))
		})
		It("should be able to unmarshal a with a string type", func() {
			var id pubsub.StreamID
			json.Unmarshal([]byte(`{"topic":"f32","type":"string"}`), &id)
			Expect(id.TopicType).To(Equal(reflect.TypeOf("")))
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

			Expect(string(b)).To(ContainSubstring("int"))
			Expect(err).To(BeNil())
		})
	})

})
