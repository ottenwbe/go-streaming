package main

import (
	"fmt"
	"sync"
	"time"
)

var len = 100000

func main() {

	result1 := make(chan time.Duration)
	result2 := make(chan time.Duration)
	result3 := make(chan time.Duration)

	sim3(result3)
	sim2(result2)
	sim1(result1)

	fmt.Printf("1: %v, 2: %v, 3: %v", <-result1, <-result2, <-result3)
}

func sim1(result chan time.Duration) {

	var (
		input = make(chan int)
		start = time.Now()
	)

	go func() {
		for i := 0; i < len; i++ {
			e := <-input
			fmt.Printf("1: %v\n", e)
		}
		result <- time.Now().Sub(start)
	}()
	go func() {
		for i := 0; i < len; i++ {
			input <- i
		}
	}()
}

func sim2(result chan time.Duration) {
	var input = make(chan int)
	var output = make(chan int)
	var start = time.Now()

	go func() {
		for i := 0; i < len; i++ {
			output <- <-input
		}
	}()
	go func() {
		for i := 0; i < len; i++ {
			e := <-output
			fmt.Printf("2: %v\n", e)
		}
		result <- time.Now().Sub(start)
	}()
	go func() {
		for i := 0; i < len; i++ {
			input <- i
		}
	}()
}

func sim3(result chan time.Duration) {
	var input = make(chan int)
	var start = time.Now()

	go func() {
		for i := 0; i < len; i++ {
			e := <-input
			fmt.Printf("3: %v\n", e)

		}
		result <- time.Now().Sub(start)
	}()

	var m sync.Mutex

	go func() {
		for i := 0; i < len/2; i++ {
			func() {
				m.Lock()
				defer m.Unlock()

				input <- i
			}()
		}
	}()

	go func() {
		for i := 0; i < len/2; i++ {
			func() {
				m.Lock()
				defer m.Unlock()

				input <- i + len/2
			}()
		}
	}()
}
