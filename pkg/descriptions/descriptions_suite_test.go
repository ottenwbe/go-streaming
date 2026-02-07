package descriptions_test

import (
	"reflect"
	"testing"

	"github.com/ottenwbe/go-streaming/internal/engine"
	"github.com/ottenwbe/go-streaming/pkg/descriptions"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

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
			
		})
	})
})
