package events

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
)

var defaultBufferCapacity = 5

var (
	ErrBufferStopped = errors.New("buffer: is stopped")
	ErrLimitExceeded = errors.New("buffer: limit exceeded")
)

// BufferReader allows read-only access to an underlying event buffer that implements BufferReader
type BufferReader[T any] interface {
	Get(i int) Event[T]
	Len() int
}

// EventBuffer is a simple, non-concurrent, in-memory buffer for events.
// It is not safe for concurrent use without external locking. It implements BufferReader.
type EventBuffer[T any] struct {
	events []Event[T]
}

// NewEventBuffer creates a new, empty EventBuffer.
func NewEventBuffer[T any]() *EventBuffer[T] {
	return &EventBuffer[T]{
		events: make([]Event[T], 0),
	}
}

// Get returns the event at the given index.
func (b *EventBuffer[T]) Get(i int) Event[T] {
	return b.events[i]
}

// Len returns the number of events in the buffer.
func (b *EventBuffer[T]) Len() int {
	return len(b.events)
}

// Add appends an event to the buffer.
func (b *EventBuffer[T]) Add(e Event[T]) {
	b.events = append(b.events, e)
}

// Remove removes the first n events from the buffer.
func (b *EventBuffer[T]) Remove(n int) {
	if n <= 0 {
		return
	}
	if n > len(b.events) {
		n = len(b.events)
	}
	for i := 0; i < n; i++ {
		b.events[i] = nil
	}
	b.events = b.events[n:]
}

// basicBuffer is an (internal) alias for an event array
type basicBuffer[T any] []Event[T]

// Buffer interface to interact with any buffer
type Buffer[T any] interface {
	GetAndConsumeNextEvents(ctx context.Context) ([]Event[T], error)
	PeekNextEvent(ctx context.Context) (Event[T], error)
	AddEvent(ctx context.Context, event Event[T]) error
	AddEvents(ctx context.Context, events []Event[T]) error
	Len() int
	Get(x int) Event[T]
	Dump() []Event[T]
	StopBlocking()
	StartBlocking()
	GetAndRemoveNextEvent(ctx context.Context) (Event[T], error)
}

type Iterator[T any] struct {
	x      int
	buffer Buffer[T]
}

// asyncBuffer represents an asynchronous buffer.
// This is the base for all other buffers, like the SimpleAsyncBuffer
type asyncBuffer[T any] struct {
	buffer      basicBuffer[T]
	id          uuid.UUID
	bufferMutex sync.Mutex
	signal      chan struct{}
	stopped     bool
}

// SimpleAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations GetNextEvent and RemoveNextEvent either return the Next event,
// if any is available in the buffer or wait until Next event is available.
type SimpleAsyncBuffer[T any] struct {
	*asyncBuffer[T]
}

// SortedSimpleAsyncBuffer is a buffer that ensures events are strictly ordered by their StartTime.
type SortedSimpleAsyncBuffer[T any] struct {
	*SimpleAsyncBuffer[T]
	limit int
}

// LimitedSimpleAsyncBuffer is a buffer with a fixed capacity limit.
type LimitedSimpleAsyncBuffer[T any] struct {
	*SimpleAsyncBuffer[T]
	limit int
}

// LimitedConsumableAsyncBuffer is a buffer with a fixed capacity limit that consumes events based on a policy.
type LimitedConsumableAsyncBuffer[T any] struct {
	*ConsumableAsyncBuffer[T]
	limit int
}

// ConsumableAsyncBuffer allows to sync exactly one reader and n writer.
// The Read operations PeekNextEvent and RemoveNextEvent either return the Next event,
// if any is available in the buffer or wait until Next event is available based on a selection policy.
// see selection.SelectionPolicy[T]
type ConsumableAsyncBuffer[T any] struct {
	*asyncBuffer[T]
	selectionPolicy SelectionPolicy[T]
}

func (b basicBuffer[T]) Get(i int) Event[T] {
	return b[i]
}

func (b basicBuffer[T]) Len() int {
	return len(b)
}

func newAsyncBuffer[T any]() *asyncBuffer[T] {
	s := &asyncBuffer[T]{
		buffer:      make(basicBuffer[T], 0, defaultBufferCapacity),
		bufferMutex: sync.Mutex{},
		stopped:     false,
		signal:      make(chan struct{}),
		id:          uuid.New(),
	}
	return s
}

// NewSimpleAsyncBuffer creates a new unbounded asynchronous buffer.
func NewSimpleAsyncBuffer[T any]() Buffer[T] {
	s := &SimpleAsyncBuffer[T]{
		asyncBuffer: newAsyncBuffer[T](),
	}
	return s
}

// NewSortedSimpleAsyncBuffer creates a new asynchronous buffer that sorts events by timestamp on insertion.
// A limit <= 0 means the buffer is unbounded.
func NewSortedSimpleAsyncBuffer[T any](limit int) Buffer[T] {
	s := &SortedSimpleAsyncBuffer[T]{
		SimpleAsyncBuffer: &SimpleAsyncBuffer[T]{
			asyncBuffer: newAsyncBuffer[T](),
		},
		limit: limit,
	}
	return s
}

// NewLimitedSimpleAsyncBuffer creates a new asynchronous buffer with a maximum event limit.
func NewLimitedSimpleAsyncBuffer[T any](limit int) Buffer[T] {
	s := &LimitedSimpleAsyncBuffer[T]{
		SimpleAsyncBuffer: &SimpleAsyncBuffer[T]{
			asyncBuffer: newAsyncBuffer[T](),
		},
		limit: limit,
	}
	return s
}

// NewLimitedConsumableAsyncBuffer creates a new buffer that consumes events based on a selection policy with a maximum event limit.
func NewLimitedConsumableAsyncBuffer[T any](policy SelectionPolicy[T], limit int) Buffer[T] {
	s := &LimitedConsumableAsyncBuffer[T]{
		ConsumableAsyncBuffer: &ConsumableAsyncBuffer[T]{
			asyncBuffer:     newAsyncBuffer[T](),
			selectionPolicy: policy,
		},
		limit: limit,
	}
	return s
}

// NewConsumableAsyncBuffer creates a new buffer that consumes events based on a selection policy.
func NewConsumableAsyncBuffer[T any](policy SelectionPolicy[T]) Buffer[T] {
	s := &ConsumableAsyncBuffer[T]{
		asyncBuffer:     newAsyncBuffer[T](),
		selectionPolicy: policy,
	}
	return s
}

func (s *asyncBuffer[T]) StartBlocking() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	s.stopped = false
}

func (s *asyncBuffer[T]) StopBlocking() {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return
	}
	s.stopped = true
	s.broadcast()
}

func (s *asyncBuffer[T]) broadcast() {
	close(s.signal)
	s.signal = make(chan struct{})
}

func (s *asyncBuffer[T]) wait(ctx context.Context) error {
	sig := s.signal
	s.bufferMutex.Unlock()
	select {
	case <-sig:
		s.bufferMutex.Lock()
		return nil
	case <-ctx.Done():
		s.bufferMutex.Lock()
		return ctx.Err()
	}
}

func (s *asyncBuffer[T]) Len() int {
	return len(s.buffer)
}

func (s *asyncBuffer[T]) Get(i int) Event[T] {
	return s.buffer[i]
}

func (s *asyncBuffer[T]) Dump() []Event[T] {
	destination := make([]Event[T], s.Len())
	copy(destination, s.buffer)
	return destination
}

// PeekNextEvent returns the Next buffered event, but no event will be removed from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *asyncBuffer[T]) PeekNextEvent(ctx context.Context) (Event[T], error) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	for s.Len() == 0 && !s.stopped {
		if err := s.wait(ctx); err != nil {
			return nil, err
		}
	}

	if s.Len() > 0 {
		return s.buffer[0], nil
	}
	return nil, nil
}

func (s *asyncBuffer[T]) GetAndRemoveNextEvent(ctx context.Context) (Event[T], error) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	var e Event[T] = nil

	for s.Len() == 0 && !s.stopped {
		if err := s.wait(ctx); err != nil {
			return nil, err
		}
	}

	if s.Len() > 0 {
		e = s.buffer[0]
		s.buffer[0] = nil
		s.buffer = s.buffer[1:]
		return e, nil
	}

	return nil, nil
}

// GetAndConsumeNextEvents returns the Next event from the buffer and removes it.
func (s *SimpleAsyncBuffer[T]) GetAndConsumeNextEvents(ctx context.Context) ([]Event[T], error) {
	e, err := s.asyncBuffer.GetAndRemoveNextEvent(ctx)
	if err != nil {
		return nil, err
	}
	if e != nil {
		return []Event[T]{e}, nil
	}
	return nil, nil
}

// AddEvents adds multiple events to the buffer.
func (s *SimpleAsyncBuffer[T]) AddEvents(ctx context.Context, events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	s.buffer = append(s.buffer, events...)
	s.broadcast()
	return nil
}

// AddEvent adds a single event to the buffer.
func (s *SimpleAsyncBuffer[T]) AddEvent(ctx context.Context, event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	s.buffer = append(s.buffer, event)
	s.broadcast()
	return nil
}

// AddEvents adds multiple events to the buffer and sorts them by StartTime.
func (s *SortedSimpleAsyncBuffer[T]) AddEvents(ctx context.Context, events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	if s.limit > 0 {
		if len(events) > s.limit {
			return fmt.Errorf("%w: %d > %d", ErrLimitExceeded, len(events), s.limit)
		}
		for s.buffer.Len()+len(events) > s.limit {
			if err := s.wait(ctx); err != nil {
				return err
			}
			if s.stopped {
				return ErrBufferStopped
			}
		}
	}

	s.buffer = append(s.buffer, events...)

	// Sort the buffer based on StartTime
	sort.SliceStable(s.buffer, func(i, j int) bool {
		return s.buffer[i].GetStamp().StartTime.Before(s.buffer[j].GetStamp().StartTime)
	})

	s.broadcast()
	return nil
}

// AddEvent adds a single event to the buffer and sorts it into position.
func (s *SortedSimpleAsyncBuffer[T]) AddEvent(ctx context.Context, event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	if s.limit > 0 {
		for s.buffer.Len() >= s.limit {
			if err := s.wait(ctx); err != nil {
				return err
			}
			if s.stopped {
				return ErrBufferStopped
			}
		}
	}

	s.buffer = append(s.buffer, event)
	sort.SliceStable(s.buffer, func(i, j int) bool {
		return s.buffer[i].GetStamp().StartTime.Before(s.buffer[j].GetStamp().StartTime)
	})

	s.broadcast()
	return nil
}

// GetAndConsumeNextEvents returns the Next event from the buffer and removes it.
func (s *SortedSimpleAsyncBuffer[T]) GetAndConsumeNextEvents(ctx context.Context) ([]Event[T], error) {
	nextEvents, err := s.SimpleAsyncBuffer.GetAndConsumeNextEvents(ctx)
	if err != nil {
		return nil, err
	}
	if s.limit > 0 && len(nextEvents) > 0 {
		s.bufferMutex.Lock()
		defer s.bufferMutex.Unlock()
		s.broadcast()
	}
	return nextEvents, nil
}

// GetAndRemoveNextEvent returns the Next event from the buffer and removes it.
func (s *SortedSimpleAsyncBuffer[T]) GetAndRemoveNextEvent(ctx context.Context) (Event[T], error) {
	event, err := s.SimpleAsyncBuffer.GetAndRemoveNextEvent(ctx)
	if err != nil {
		return nil, err
	}
	if s.limit > 0 && event != nil {
		s.bufferMutex.Lock()
		defer s.bufferMutex.Unlock()
		s.broadcast()

	}
	return event, nil
}

// GetAndConsumeNextEvents returns the Next buffered events and removes this event from the buffer.
// Blocks until at least one event buffered.
// When stopped, returns nil.
func (s *ConsumableAsyncBuffer[T]) GetAndConsumeNextEvents(ctx context.Context) ([]Event[T], error) {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	var (
		selectedEvents basicBuffer[T]
		selectionFound = s.selectionPolicy.NextSelectionReady(s.buffer)
		selection      EventSelection
	)

	// Wait until the buffer has enough events
	for !selectionFound && !s.stopped {
		if err := s.wait(ctx); err != nil {
			return nil, err
		}
		selectionFound = s.selectionPolicy.NextSelectionReady(s.buffer)
	}

	selection = s.selectionPolicy.NextSelection()
	if selection.IsValid() { // Selection is in some cases not valid if s.stopped
		// Extract selected events from the buffer
		src := s.buffer[selection.Start : selection.End+1]
		selectedEvents = make([]Event[T], len(src))
		copy(selectedEvents, src)
	}

	if selectionFound {
		s.selectionPolicy.Shift()
		s.selectionPolicy.UpdateSelection(s.buffer)
		offset := s.selectionPolicy.NextSelection().Start

		removeCount := offset
		if removeCount > len(s.buffer) {
			removeCount = len(s.buffer)
		}

		for i := 0; i < removeCount; i++ {
			s.buffer[i] = nil
		}
		s.buffer = s.buffer[removeCount:]
		s.selectionPolicy.Offset(removeCount)

		return selectedEvents, nil
	}
	return nil, nil
}

// AddEvents adds multiple events to the buffer and updates the selection policy.
func (s *ConsumableAsyncBuffer[T]) AddEvents(ctx context.Context, events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	s.buffer = append(s.buffer, events...)
	s.selectionPolicy.UpdateSelection(s.buffer)

	s.broadcast()
	return nil
}

// AddEvent adds a single event to the buffer and updates the selection policy.
func (s *ConsumableAsyncBuffer[T]) AddEvent(ctx context.Context, event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	s.buffer = append(s.buffer, event)
	s.selectionPolicy.UpdateSelection(s.buffer)

	s.broadcast()
	return nil
}

// AddEvents adds multiple events to the buffer, ensuring the limit is not exceeded.
func (s *LimitedSimpleAsyncBuffer[T]) AddEvents(ctx context.Context, events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if len(events) > s.limit {
		return fmt.Errorf("%w: %d > %d", ErrLimitExceeded, len(events), s.limit)
	}
	if s.stopped {
		return ErrBufferStopped
	}

	for s.buffer.Len()+len(events) > s.limit {
		if err := s.wait(ctx); err != nil {
			return err
		}
		if s.stopped {
			return ErrBufferStopped
		}
	}

	s.buffer = append(s.buffer, events...)
	s.broadcast()
	return nil
}

// AddEvent adds a single event to the buffer, ensuring the limit is not exceeded.
func (s *LimitedSimpleAsyncBuffer[T]) AddEvent(ctx context.Context, event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	for s.buffer.Len() >= s.limit {
		if err := s.wait(ctx); err != nil {
			return err
		}
		if s.stopped {
			return ErrBufferStopped
		}
	}

	s.buffer = append(s.buffer, event)
	s.broadcast()
	return nil
}

// GetAndConsumeNextEvents returns the Next event from the buffer and removes it.
func (s *LimitedSimpleAsyncBuffer[T]) GetAndConsumeNextEvents(ctx context.Context) ([]Event[T], error) {
	nextEvents, err := s.SimpleAsyncBuffer.GetAndConsumeNextEvents(ctx)
	if err != nil {
		return nil, err
	}

	if len(nextEvents) > 0 {
		s.bufferMutex.Lock()
		defer s.bufferMutex.Unlock()
		s.broadcast()

	}
	return nextEvents, nil
}

// GetAndRemoveNextEvent returns the Next event from the buffer and removes it.
func (s *LimitedSimpleAsyncBuffer[T]) GetAndRemoveNextEvent(ctx context.Context) (Event[T], error) {
	event, err := s.SimpleAsyncBuffer.GetAndRemoveNextEvent(ctx)
	if err != nil {
		return nil, err
	}
	if event != nil {
		s.bufferMutex.Lock()
		defer s.bufferMutex.Unlock()
		s.broadcast()

	}
	return event, nil
}

// AddEvents adds multiple events to the buffer, ensuring the limit is not exceeded.
func (s *LimitedConsumableAsyncBuffer[T]) AddEvents(ctx context.Context, events []Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if len(events) > s.limit {
		return fmt.Errorf("%w: %d > %d", ErrLimitExceeded, len(events), s.limit)
	}
	if s.stopped {
		return ErrBufferStopped
	}

	for s.buffer.Len()+len(events) > s.limit {
		if err := s.wait(ctx); err != nil {
			return err
		}
		if s.stopped {
			return ErrBufferStopped
		}
	}

	s.buffer = append(s.buffer, events...)
	s.selectionPolicy.UpdateSelection(s.buffer)

	s.broadcast()
	return nil
}

// AddEvent adds a single event to the buffer, ensuring the limit is not exceeded.
func (s *LimitedConsumableAsyncBuffer[T]) AddEvent(ctx context.Context, event Event[T]) error {
	s.bufferMutex.Lock()
	defer s.bufferMutex.Unlock()

	if s.stopped {
		return ErrBufferStopped
	}

	for s.buffer.Len() >= s.limit {
		if err := s.wait(ctx); err != nil {
			return err
		}
		if s.stopped {
			return ErrBufferStopped
		}
	}

	s.buffer = append(s.buffer, event)
	s.selectionPolicy.UpdateSelection(s.buffer)

	s.broadcast()
	return nil
}

// GetAndConsumeNextEvents returns the Next event from the buffer and removes it.
func (s *LimitedConsumableAsyncBuffer[T]) GetAndConsumeNextEvents(ctx context.Context) ([]Event[T], error) {
	nextEvents, err := s.ConsumableAsyncBuffer.GetAndConsumeNextEvents(ctx)
	if err != nil {
		return nil, err
	}

	if len(nextEvents) > 0 {
		s.bufferMutex.Lock()
		defer s.bufferMutex.Unlock()
		s.broadcast()
	}
	return nextEvents, nil
}

func (s *LimitedConsumableAsyncBuffer[T]) GetAndRemoveNextEvent(ctx context.Context) (Event[T], error) {
	e, err := s.ConsumableAsyncBuffer.GetAndRemoveNextEvent(ctx)
	if err != nil {
		return nil, err
	}
	if e != nil {
		s.bufferMutex.Lock()
		s.broadcast()
		s.bufferMutex.Unlock()
	}
	return e, nil
}

func (i *Iterator[T]) HasNext() bool {
	return i.x < i.buffer.Len()
}

func (i *Iterator[T]) Next() Event[T] {
	e := i.buffer.Get(i.x)
	i.x++
	return e
}

func NewIterator[T any](buffer Buffer[T]) *Iterator[T] {
	return &Iterator[T]{
		x:      0,
		buffer: buffer,
	}
}
