# Go Streaming Library
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/ottenwbe/go-streaming/blob/main/LICENSE)
[![Go](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml/badge.svg)](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml)

This is a basic event streaming and processing library.
The processing engine is mainly designed for flexibility and extendability  .
It provides a lightweight, generic, in-memory, type-safe streaming and event processing library for Go.


## Disclaimer
This project's main purpose was to understand concurrency in go by the authors; channels, mutexes, etc, not the functionality of the stream processing system.
Hence, also no specific language like CQL is supported and optimizations are not in focus.

## Features

- **Type-Safe Pub/Sub**: Leverages Go generics for type-safe event streams.
- **Continuous Queries**: Functional DSL for building stream processing pipelines.
- **Backpressure & Buffering**: Built-in support for buffered streams and flow control.
- **Windowing**: Support for time-based and count-based windows (via selection policies).

## Limitations

- **Operator Support**: The primary supported operator types are the `PIPELINE_OPERATOR`, `FILTER` and `MAP` for batch processing. -  **Standard Operators**: The set of standard, built-in operators is minimal and will need to be expanded for more complex use cases.
- **Joins and Multi-Stream Windows**: The current DSL and engine do not support operators with multiple input streams (e.g., joins) or windowing across them.
- **Builder API**: The Query creation using a fluent builder pattern for query construction is experimental.

## Usage

### Pub/Sub

```go
// Subscribe to a topic
sub, err := pubsub.SubscribeByTopic[int]("my-topic", func(e events.Event[int]) {
    fmt.Println("Received:", e.GetContent())
})

// Publish to a topic
err := pubsub.InstantPublishByTopic[int]("my-topic", 42)
```

### Continuous Queries

Build processing pipelines using the functional API:

```go
q, err := query.Query[int](
    query.Process[int](
        engine.ContinuousSmaller[int](50), // Example operator
        query.FromSourceStream[int]("source-topic"),
    ),
)

q.Subscribe(func(e events.Event[int]) {
    // Handle processed output
})

q.Run()
```

## Examples

See our examples folder for more details.


## Packages

- `pubsub`: Core messaging infrastructure.
- `query`: Query construction and lifecycle management.
- `engine`: Stream processing operators.
- `buffer`: Event buffering implementations.