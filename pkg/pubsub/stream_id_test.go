package pubsub_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go-stream-processing/pkg/pubsub"
)

var _ = Describe("StreamID", func() {
	Describe("IsNil method", func() {
		It("should return true for nilID", func() {
			id := pubsub.NilStreamID()
			Expect(id.IsNil()).To(BeTrue())
		})

		It("should return false for a valid string", func() {
			id := pubsub.StreamID("valid_stream_id")
			Expect(id.IsNil()).To(BeFalse())
		})

		It("should return true for an empty string", func() {
			id := pubsub.StreamID("")
			Expect(id.IsNil()).To(BeTrue())
		})
	})

	Describe("String method", func() {
		It("should return the underlying string", func() {
			id := pubsub.StreamID("test_id")
			Expect(id.String()).To(Equal("test_id"))
		})
	})

	Describe("NilStreamID function", func() {
		It("should return the nilID constant", func() {
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
})
