package engine_test

import (
	"github.com/google/uuid"
	"go-stream-processing/internal/engine"
	"go-stream-processing/internal/events"
	"go-stream-processing/internal/pubsub"
	"testing"

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
		streamIn := pubsub.NewLocalSyncStream[int](pubsub.NewStreamDescription("int values", uuid.New(), false))
		streamOut := pubsub.NewLocalSyncStream[int](pubsub.NewStreamDescription("summed up values", uuid.New(), false))

		inStream := engine.NewSingleStreamInput1[int](streamIn.ID())

		smaller := func(input engine.SingleStreamSelection1[int]) []events.Event[int] {
			if input.GetContent() < 11 {
				return []events.Event[int]{input}
			} else {
				return []events.Event[int]{}
			}

		}

		op = engine.NewOperatorN[engine.SingleStreamSelection1[int], int](smaller, inStream, streamOut)
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