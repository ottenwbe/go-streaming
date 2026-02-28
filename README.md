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
- **Windowing**: Support for time-based (`TemporalWindow`) and count-based (`CountingWindow`) windows.
- **Joins**: Support for joining streams (`Join`, `LeftJoin`) with windowing policies.

## Limitations

- **Operator Support**: The library provides a set of standard operators (`Filter`, `Map`, `Join`, `BatchSum`, etc.), but specialized operators may need to be implemented.
- **Distributed Processing**: This is currently a single-node, in-memory processing engine.

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
b := processing.NewBuilder[int]()
b.From(processing.Source[int]("source-topic")).
    Process(processing.Operator[int](processing.Smaller[int](50)))

q, err := b.Build(true)

q.Subscribe(func(e events.Event[int]) {
    // Handle processed output
})

q.Run()
```

## Examples

See our examples folder for more details.


## Packages

- `pkg/pubsub`: Core messaging infrastructure.
- `pkg/processing`: Query construction, lifecycle management, and operators.
- `pkg/events`: Event definitions and buffering implementations.
- `pkg/log`: Logging interface.