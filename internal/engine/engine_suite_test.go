package engine_test

import (
	"testing"

	"github.com/ottenwbe/go-streaming/internal/engine"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEngine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Engine Suite")
}

var _ = Describe("OperatorRepository", func() {

	var (
		op engine.OperatorControl
	)

	BeforeEach(func() {
		streamInID, _ := pubsub.GetOrAddStream[int]("int values")
		streamOutID, _ := pubsub.GetOrAddStream[int]("summed up values")
		defer pubsub.ForceRemoveStream(streamOutID, streamOutID)

		inStream := engine.NewSingleStreamInput1[int](streamInID)

		smaller := func(input engine.SingleStreamSelection1[int]) []events.Event[int] {
			if input.GetContent() < 11 {
				return []events.Event[int]{input}
			} else {
				return []events.Event[int]{}
			}

		}

		op = engine.NewOperatorN[engine.SingleStreamSelection1[int], int](smaller, inStream, streamOutID)
	})

	Context("Get and put", func() {
		It("adds new operators to the map and retrieves it", func() {
			err := engine.OperatorRepository().Put(op)
			oResult, ok := engine.OperatorRepository().Get(op.ID())

			Expect(err).To(BeNil())
			Expect(ok).To(BeTrue())
			Expect(op.ID()).To(Equal(oResult.ID()))
		})
		It("does not allow duplicated operators", func() {
			engine.OperatorRepository().Put(op)
			err := engine.OperatorRepository().Put(op)
			Expect(err).ToNot(BeNil())
		})
		It("does not allow nil operators", func() {
			err := engine.OperatorRepository().Put(nil)
			Expect(err).ToNot(BeNil())
		})
	})
	Context("List", func() {
		It("can be listed", func() {
			engine.OperatorRepository().Put(op)
			l := engine.OperatorRepository().List()
			Expect(l).To(ContainElement(op))
		})
	})
})
