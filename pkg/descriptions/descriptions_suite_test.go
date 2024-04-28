package descriptions_test

import (
	"go-stream-processing/internal/engine"
	"go-stream-processing/pkg/descriptions"
	"go-stream-processing/pkg/pubsub"
	"reflect"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDescriptions(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Descriptions Suite")
}

var _ = Describe("Operator Description", func() {

	Context("Define", func() {
		It("can be created and values can be read afterwards", func() {
			q := descriptions.NewQueryDescription()

			o := descriptions.OperatorDescription{
				ID:     engine.OperatorID{},
				Inputs: nil,
				Output: pubsub.MakeStreamID[any](""),
				Engine: 0,
			}

			q.Operators = append(q.Operators, o)

			var t reflect.Type = reflect.TypeOf(1)

			_ = reflect.New(t)

			//s := pubsub.StreamReceiver[reflect.]{}
			//			fmt.Println(i.Recv())

		})
	})
})
