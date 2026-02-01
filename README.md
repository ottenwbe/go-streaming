# Go-Stream-Processing
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/ottenwbe/go-streaming/blob/main/LICENSE)
[![Go](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml/badge.svg)](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml)

This project is mainly created to understand concurrency in go. 
That said the idea is to build a simple event streaming playground.
The processing engine is mainly designed for flexibility and extendability to test (GoLang) features.
It provides a lightweight, in-memory Pub/Sub system and a Continuous Query engine. Hence, also no specific language like CQL is supported.

## Features

*   **Type-Safe Pub/Sub**: Generic implementation allowing streams of specific types (e.g., `Stream[int]`, `Stream[string]`).
*   **Concurrency Models**: Supports both synchronous and asynchronous (buffered) stream processing.
*   **Continuous Queries**: Build complex processing pipelines using a builder pattern.
*   **Windowing**: Built-in support for:
    *   **Counting Windows**: Aggregate over the last *N* events.
    *   **Temporal Windows**: Aggregate over time durations (e.g., last 5 minutes).
*   **Operators**: Standard operators like Sum, Count, etc.

## Quick Start

### 1. Basic Pub/Sub

Create a stream, subscribe to it, and publish events.

```go
package main

import (
	"fmt"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
)

func main() {
	// 1. Define and register a stream
	topic := "greetings"
	// Stream of strings, synchronous (false), multiple publishers allowed (false for singleFanIn)
	desc := pubsub.MakeStreamDescription[string](topic, false, false)
	pubsub.AddOrReplaceStreamFromDescription[string](desc)

	// 2. Subscribe
	// MakeStreamID creates a typed ID for the topic
	streamID := pubsub.MakeStreamID[string](topic)
	receiver, _ := pubsub.Subscribe[string](streamID)

	// 3. Publish
	publisher, _ := pubsub.RegisterPublisher[string](streamID)
	publisher.Publish(events.NewEvent("Hello World!"))

	// 4. Consume
	event := <-receiver.Notify()
	fmt.Printf("Received: %s\n", event.GetContent())
}
```

### 2. Continuous Queries & Windowing

Process streams using continuous queries. This example calculates a rolling sum of integers.

```go
package main

import (
	"fmt"
	"go-stream-processing/pkg/events"
	"go-stream-processing/pkg/pubsub"
	"go-stream-processing/pkg/query"
	"go-stream-processing/pkg/selection"
)

func main() {
	inputTopic := "numbers"
	outputTopic := "sum-result"

	// 1. Create the Query Builder
	b := query.NewBuilder()

	// 2. Define Input Stream
	b.Stream(query.S[int](inputTopic, true, false))

	// 3. Define Window Policy (Sum last 5 elements, slide by 1)
	window := selection.NewCountingWindowPolicy[int](5, 1)

	// 4. Add Query Operation (Batch Sum)
	b.Query(query.ContinuousBatchSum[int](inputTopic, outputTopic, window))

	// 5. Build and Run
	q, _ := b.Build()
	// RunAndSubscribe starts the query and gives us a receiver for the output topic
	qs, _ := query.RunAndSubscribe[int](q)
	defer query.Close(qs)

	// ... Publish events to 'numbers' topic ...
	// ... Read results from qs.Notify() ...
}
```

## Development

Tests can be executed as follows

````
ginkgo -cover -v  ./... 
````