package datasink

import (
	"time"
)

type BucketType int

const (
	HistoricalBucketType BucketType = iota + 1
	DeltaBucketType

	NilBucketType
)

func ParseBucketType(input string) (BucketType, error) {
	bucketType, ok := map[string]BucketType{
		"historical": HistoricalBucketType,
		"delta":      DeltaBucketType,
	}[input]

	if !ok {
		return NilBucketType, ErrInvalidBucketType{input}
	}
	return bucketType, nil
}

type EventDayBucketAggregatorOptions struct {
	BucketType BucketType
	BufferSize int
}

func NewDefaultDayBucketAggregatorOptions() EventDayBucketAggregatorOptions {
	return EventDayBucketAggregatorOptions{
		BucketType: DeltaBucketType,
		BufferSize: 100,
	}
}

func NewEventDayBucketAggregator(inputCh chan GithubRepositoryEventDayBucket, opts EventDayBucketAggregatorOptions) EventDayBucketAggregator {
	if opts == (EventDayBucketAggregatorOptions{}) {
		opts = NewDefaultDayBucketAggregatorOptions()
	}

	return &eventDayBucketAggregator{
		opts: opts,

		inputCh:  inputCh,
		outputCh: make(chan AggregateEventDayBucket, opts.BufferSize),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

type eventDayBucketAggregator struct {
	opts           EventDayBucketAggregatorOptions
	inputCh        chan GithubRepositoryEventDayBucket
	outputCh       chan AggregateEventDayBucket
	stopCh, doneCh chan struct{}
}

func (e *eventDayBucketAggregator) Init() {
	go e.runLoop()
}

func (e *eventDayBucketAggregator) runLoop() {
	defer close(e.doneCh)
	defer close(e.outputCh)

	var currentTs time.Time
	counts := make(map[int]int)
	events := make([]GithubRepositoryEventDayBucket, 0)

	flush := func() {
		if len(events) == 0 {
			return
		}

		e.outputCh <- AggregateEventDayBucket{
			DayBucketTs: currentTs,
			Events:      events,
		}

		// if this is a delta bucket type (not historical) then all
		// values must be cleared out between flushes
		if e.opts.BucketType == DeltaBucketType {
			for k, _ := range counts {
				counts[k] = 0
			}
		}

		events = events[:0]
	}
	defer flush()

	for {
		select {
		case <-e.stopCh:
			return
		case event, ok := <-e.inputCh:
			if !ok {
				return
			}

			if currentTs.Before(event.DayBucketTs) {
				flush()
				currentTs = event.DayBucketTs
			}

			events = append(events, event)
			origCount, ok := counts[event.RepositoryId]
			if !ok {
				origCount = 0
			}
			count := origCount + event.Count
			counts[event.RepositoryId] = count
			event.Count = count
		}
	}
}

func (e *eventDayBucketAggregator) OutputCh() chan AggregateEventDayBucket {
	return e.outputCh
}

func (e *eventDayBucketAggregator) Close() {
	select {
	case e.stopCh <- struct{}{}:
	default:
		return
	}

	<-e.doneCh
}
