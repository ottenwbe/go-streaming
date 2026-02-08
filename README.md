# Go-Stream-Processing
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/ottenwbe/go-streaming/blob/main/LICENSE)
[![Go](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml/badge.svg)](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml)

This project is created as a playground to understand concurrency in go. 
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

## Pub/Sub System

The core of this library is a generic Pub/Sub system. It allows you to create streams that carry specific data types (e.g., `int`, `string`, `structs`).
Subscribers will get notified about new events published to the stream. Publishers can send events to dedicated streams.

### Minimal Example

```go
package main

import (
	"fmt"
	"github.com/ottenwbe/go-streaming/pkg/events"
	"github.com/ottenwbe/go-streaming/pkg/pubsub"
)

func main() {
	// 1. Subscribe to a topic 
	sub, _ := pubsub.SubscribeByTopic[int]("my-topic")

	// 2. Publish to the same topic
	pub, _ := pubsub.RegisterPublisherByTopic[int]("my-topic")
	pub.Publish(events.NewEvent(42))

	// 3. Consume the event
	event, _ := sub.Consume()
	fmt.Printf("Received: %v\n", event.GetContent())

	// 4. Cleanup to avoid resource leaks
	pubsub.UnRegisterPublisher(pub)
	pubsub.Unsubscribe(sub)
}
```

### Stream Descriptions

Streams are configured using a `StreamDescription`. You can customize the behavior of the stream, e.g. regarding concurrency and buffering. 

```go
// Create a simple synchronous stream for integers
desc := pubsub.MakeStreamDescription[int]("my-topic")

// Create an asynchronous stream with specific options
descAsync := pubsub.MakeStreamDescription[string]("my-async-topic",
    pubsub.WithAsyncStream(true),       // Decouple publishers from the stream
    pubsub.WithAsyncReceiver(true),     // Decouple subscribers from the stream
    pubsub.WithBufferCapacity(100),     // Set buffer size to 100 events
)
```

**Configuration Options:**

*   **`WithAsyncStream(bool)`**: If `true`, publishers write to an input channel, and the stream processes events in a background goroutine. Default is `false` (synchronous).
*   **`WithAsyncReceiver(bool)`**: If `true`, the subscriber has its own buffer. The stream dispatches to the buffer and moves on immediately. Default is `false` (synchronous dispatch).
*   **`WithBufferCapacity(int)`**: Sets the size of the internal buffer for async streams or receivers. If set to a value > 0, **backpressure** is applied (the writer blocks when full). If 0 (default), the buffer is unbounded.



## Quick Start

Check out the examples in the example section

## Development

Tests can be executed as follows

````
ginkgo -cover -v  ./... 
````