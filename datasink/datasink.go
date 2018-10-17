package datasink

import (
	"context"
	"time"
)

// This module exposes the primary interfaces and types for the datasink
// package. All errors are found in the errors.go module and specialized types
// such as implementation specific options or bucket types are found in their
// respective modules

// The github archive project only has data from Feb 2011 and onward:
// https://bigquery.cloud.google.com/table/githubarchive:month.201102
const EarliestDate = "20110102"

// GithubRepositoryEventDayBucket represents a day bucket for events for a repository
type GithubRepositoryEventDayBucket struct {
	Err error

	RepositoryId int
	Count        int
	DayBucketTs  time.Time
}

// GithubRepository represents a repository and it's event count for
// whatever event was requested
type GithubRepository struct {
	Err error

	RepositoryUrl string
	RepositoryId  int
	EventCount    int
	Rank          int
}

// GithubDataSink represents a type which can fetch github data and stream it
// to a receiver channel asynchronously.
type GithubDataSink interface {
	Init() error
	Close() error

	// FetchEventCounts: fetch event counts for the provided repositories between startTS and endTS.
	FetchEventCounts([]GithubRepository, GithubEventType, time.Time, time.Time) (chan GithubRepositoryEventDayBucket, context.CancelFunc, error)

	// FetchTopNRepositories: queries Bigquery for the top N repositories
	// by the passed in EventType. Streams results via channel. Available
	// EventTypes found at http://developer.github.com/v3/activity/events/types/
	FetchTopNRepositories(int, GithubEventType) (chan GithubRepository, context.CancelFunc, error)
}

// AggregateEventDayBucket is a type which represents an aggregate set of event
// counts for a given day bucket
type AggregateEventDayBucket struct {
	DayBucketTs time.Time

	Events []GithubRepositoryEventDayBucket
}

// EventDayBucketAggregator represents a type which consumes an input channel,
// aggregates all events for a given day (assuming input ordering over the
// channel) and outputs aggregate day buckets to the output channel. It
// supports different bucketing types and can output an aggregate list of all
// deltas for a day, or an aggregate list of all historical counts for a day.
type EventDayBucketAggregator interface {
	Init()
	Close()
	OutputCh() chan AggregateEventDayBucket
}
