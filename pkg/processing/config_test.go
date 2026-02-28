package processing_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/processing"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

var _ = Describe("Config", func() {

	Describe("OperatorConfig Options", func() {
		var config *processing.OperatorConfig

		BeforeEach(func() {
			config = &processing.OperatorConfig{}
		})

		Context("WithInput", func() {
			It("should add input configurations", func() {
				input1 := processing.InputConfig{Stream: pubsub.MakeStreamID[int]("in1")}
				input2 := processing.InputConfig{Stream: pubsub.MakeStreamID[int]("in2")}

				option := processing.WithInput(input1, input2)
				option(config)

				Expect(config.Inputs).To(HaveLen(2))
				Expect(config.Inputs).To(ContainElements(input1, input2))
			})
		})

		Context("WithAutoStart", func() {
			It("should set AutoStart to true", func() {
				option := processing.WithAutoStart(true)
				option(config)
				Expect(config.AutoStart).To(BeTrue())
			})

			It("should set AutoStart to false", func() {
				config.AutoStart = true // start with non-default
				option := processing.WithAutoStart(false)
				option(config)
				Expect(config.AutoStart).To(BeFalse())
			})
		})

		Context("WithOutput", func() {
			It("should add output topics", func() {
				topic1 := pubsub.MakeStreamID[int]("out1")
				topic2 := pubsub.MakeStreamID[int]("out2")

				option := processing.WithOutput(topic1, topic2)
				option(config)

				Expect(config.Outputs).To(HaveLen(2))
				Expect(config.Outputs).To(ContainElements(topic1, topic2))
			})
		})
	})

	Describe("MakeInputConfigs", func() {
		It("should create a slice of InputConfig", func() {
			streams := []pubsub.StreamID{
				pubsub.MakeStreamID[string]("s1"),
				pubsub.MakeStreamID[string]("s2"),
			}
			policy := events.PolicyConfig{Type: events.SelectNext, Active: true}

			inputConfigs := processing.MakeInputConfigs(streams, policy)

			Expect(inputConfigs).To(HaveLen(2))
			Expect(inputConfigs[0].Stream).To(Equal(streams[0]))
			Expect(inputConfigs[0].InputPolicy).To(Equal(policy))
			Expect(inputConfigs[1].Stream).To(Equal(streams[1]))
			Expect(inputConfigs[1].InputPolicy).To(Equal(policy))
		})

		It("should return an empty slice for no input streams", func() {
			policy := events.PolicyConfig{Type: events.SelectNext}
			inputConfigs := processing.MakeInputConfigs([]pubsub.StreamID{}, policy)
			Expect(inputConfigs).To(BeEmpty())
		})
	})

	Describe("MakeOperatorConfig", func() {
		It("should create a config with default values and a new ID", func() {
			config := processing.MakeOperatorConfig(processing.FILTER_OPERATOR)

			Expect(config.Type).To(Equal(processing.FILTER_OPERATOR))
			Expect(config.ID).NotTo(Equal(processing.NilOperatorID()))
			Expect(config.AutoStart).To(BeFalse())
			Expect(config.Inputs).To(BeEmpty())
			Expect(config.Outputs).To(BeEmpty())
		})

		It("should apply provided options", func() {
			input := processing.InputConfig{Stream: pubsub.MakeStreamID[int]("in")}
			output := pubsub.MakeStreamID[int]("out")

			config := processing.MakeOperatorConfig(processing.MAP_OPERATOR, processing.WithAutoStart(true), processing.WithInput(input), processing.WithOutput(output))

			Expect(config.Type).To(Equal(processing.MAP_OPERATOR))
			Expect(config.ID).NotTo(Equal(processing.NilOperatorID()))
			Expect(config.AutoStart).To(BeTrue())
			Expect(config.Inputs).To(ConsistOf(input))
			Expect(config.Outputs).To(ConsistOf(output))
		})
	})
})
