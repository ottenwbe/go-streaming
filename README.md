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

## Quick Start

Check out the examples in the example section

## Development

Tests can be executed as follows

````
ginkgo -cover -v  ./... 
````