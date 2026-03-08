package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/log"
	"github.com/ottenwbe/go-streaming/pkg/processing"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
	"go.uber.org/zap"
)

// WordCountWindow creates a pipeline operator that counts words within a given window.
func WordCountWindow(policy events.SelectionPolicyConfig) func(in []pubsub.StreamID, out []pubsub.StreamID, id processing.OperatorID) (processing.OperatorID, error) {
	return func(in []pubsub.StreamID, out []pubsub.StreamID, id processing.OperatorID) (processing.OperatorID, error) {
		// This is the core operation that will be applied to each window of events.
		operation := func(words []events.Event[string]) []map[string]int {

			counts := make(map[string]int)
			for _, wordEvent := range words {
				counts[wordEvent.GetContent()]++
			}
			// The operator returns a slice of results. Here, we return one map for the whole window.
			return []map[string]int{counts}
		}

		// Configure the pipeline operator with the provided windowing policy.
		config := processing.MakeOperatorConfig(
			processing.PIPELINE_OPERATOR,
			processing.WithInput(processing.MakeInputConfigs(in, policy)...),
			processing.WithOutput(out...),
		)

		// Create the actual pipeline operator instance.
		return processing.NewPipelineOperator[string, map[string]int](config, operation, id)
	}
}

func main() {
	// Setup the input stream for user commands
	userInputTopic := "user-input"
	_, err := pubsub.GetOrAddStream[string](userInputTopic, pubsub.WithAsynchronousStream(true))
	if err != nil {
		zap.S().Fatalf("Failed to create user input stream: %v", err)
	}

	// --- Word Count Statistics (last 10 seconds) ---
	// Define a window of 10 seconds that shifts by 1 second
	wordCountPolicy := events.MakeSelectionPolicy(
		events.TemporalWindowOption(time.Now().Add(-10*time.Second), 10*time.Second, 1*time.Second),
	)

	b := processing.NewBuilder[map[string]int]()
	b.From(processing.Source[string](userInputTopic)).
		// Tokenize
		ConnectTo(processing.Operator[string](processing.FlatMap(func(e events.Event[string]) []string {
			return strings.Fields(e.GetContent())
		}))).
		// Count words in the window
		ConnectTo(processing.Operator[map[string]int](WordCountWindow(wordCountPolicy)))

	query, err := b.Build(true)
	if err != nil {
		zap.S().Fatalf("Failed to build query: %v", err)
	}
	// Subscribe to the output of the word count query.
	query.Subscribe(func(e events.Event[map[string]int]) {
		zap.S().Infof("Word counts for the last window (%v - %v): %v", time.Now().Add(-10*time.Second), time.Now(), e.GetContent())
	})
	defer processing.Close(query)

	// --- Main loop to "read" user input ---
	publisher, err := processing.RegisterPublisher[string](query, userInputTopic)
	if err != nil {
		zap.S().Fatalf("Failed to register publisher: %v", err)
	}
	defer processing.UnRegisterPublisher(query, publisher)

	fmt.Println("Generating random words, counting them every 10s... Press Ctrl+C to exit.")
	words := []string{"hello", "world", "foo", "bar", "streaming", "go", "rocks", "data", "pipeline"}

	for {
		// Pick a random word
		word := words[rand.IntN(len(words))]

		// Publish a TemporalEvent with the current timestamp
		evt := events.NewEvent(word)
		publisher.Publish(evt)

		time.Sleep(100 * time.Millisecond)
	}
}

func init() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	log.SetLogger(logger.Sugar())
}
