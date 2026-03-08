# Go Streaming Library

[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/ottenwbe/go-streaming/blob/main/LICENSE)
[![Go](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml/badge.svg)](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml)

This project provides a lightweight, generic, in-memory, and type-safe streaming and event processing library for Go. It is designed with flexibility and extendability in mind, allowing you to build complex data processing graphs/pipelines.

## Core Concepts

- **Events**: Events are the fundamental units of data flowing through streams. They are generic containers (`Event[T]`) that hold content and are timestamped. Defined in `pkg/events`.
- **Streams**: A stream is a type-safe, in-memory topic for publishing and subscribing to events. The core pub/sub mechanism lives in the `pkg/pubsub` package.
- **Operators**: Operators are functions that transform streams. They can be chained together to form processing graphs. The library provides a set of default operators (`Map`, `Filter`, `Join`, etc.) and allows you to easily create your own.
- **Continuous Queries**: A continuous query is a running processing graph/pipeline that processes one or more input streams through a series of operators to produce an output stream. Queries are constructed using a `Builder` located in the `pkg/processing` package.

## Project Status & Disclaimer

This project's primary purpose was for the authors to explore concurrency in Go (channels, mutexes, etc.), rather than to build a feature-complete, production-grade stream processing system. Consequently, performance optimizations and advanced features like a dedicated query language (e.g., CQL) were not the main focus.

## Features

- **Type-Safe Pub/Sub**: Leverages Go generics for type-safe event streams.
- **Continuous Queries**: Functional DSL for building stream processing graphs and pipelines.
- **Backpressure & Buffering**: Built-in support for buffered streams and flow control.
- **Windowing**: Support for time-based (`TemporalWindow`) and count-based (`CountingWindow`) windows.
- **Stream Operations**: Support for joining streams (`Join`, `LeftJoin`) with windowing policies and more.

## Limitations

- **Operator Support**: The library provides a set of standard operators (`Filter`, `Map`, `Join`, `BatchSum`, etc.), but specialized operators may need to be implemented.
- **Distributed Processing**: This is currently a single-node, in-memory processing engine.
- **Persistence**: State is not persisted. If the application restarts, all stream contents and operator states are lost.

## Installation

```sh
go get github.com/ottenwbe/go-streaming
```

## Usage

### Pub/Sub

The core `pubsub` package allows for simple, type-safe messaging.

```go
// Subscribe to a topic
sub, err := pubsub.SubscribeByTopic[int]("my-topic", func(e events.Event[int]) {
    fmt.Println("Received:", e.GetContent())
})
defer pubsub.Unsubscribe(sub)

// Publish to a topic
p, _ := pubsub.RegisterPublisher[int]("my-topic")
defer pubsub.UnRegisterPublisher(p)
p.Publish(events.NewEvent(42))
```

### Continuous Queries

Build processing pipelines using the Builder:

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