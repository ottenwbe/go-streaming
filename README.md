# Go-Stream-Processing
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/ottenwbe/go-streaming/blob/main/LICENSE)
[![Go](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml/badge.svg)](https://github.com/ottenwbe/go-streaming/actions/workflows/go.yml)

This project is mainly created to understand concurrency in go. 
That said the idea is to build a simple event streaming playground.
The processing engine is mainly designed for flexibility and extendability to test features.
Hence, also no specific language like CQL is supported.

## Examples

A few examples are in place. Check them out:

````
 go run ./examples/basics/main.go
 ````
or
````
 go run ./examples/compose/main.go
````

## Development

Tests can be executed as follows

````
ginkgo -cover -v  ./... 
````