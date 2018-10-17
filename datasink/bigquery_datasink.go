package datasink

import (
	"context"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// BigqueryDataSinkOptions represents the set of options available to the
// bigquery sink. Specifically, it includes both sink level options and options
// for the underlying bigquery client.
type BigqueryDataSinkOptions struct {
	ReadTransactionTimeout   time.Duration
	ResultsChannelBufferSize int

	ProjectId string

	// the bigquery client uses an http.Transport under the hood for
	// queries. The following http.transport specific options can be used
	// to configure the transport.
	MaxIdleConns                           int
	IdleConnTimeout, ResponseHeaderTimeout time.Duration

	// if an http.Transport is passed in, the client will be configured
	// with this and all other transport options will be ignored
	Transport *http.Transport
}

func NewDefaultBigqueryDataSinkOptions(projectId string) BigqueryDataSinkOptions {
	return BigqueryDataSinkOptions{
		ReadTransactionTimeout:   time.Minute * 10,
		ResultsChannelBufferSize: 100,

		MaxIdleConns:          20,
		IdleConnTimeout:       time.Minute,
		ResponseHeaderTimeout: time.Second * 10,
	}
}

// NewBigqueryGithubDataSink returns a datasink which uses bigquery to fetch and stream data
func NewBigqueryGithubDataSink(projectId string, opts BigqueryDataSinkOptions) GithubDataSink {
	if opts == (BigqueryDataSinkOptions{}) {
		opts = NewDefaultBigqueryDataSinkOptions(projectId)
	}

	opts.ProjectId = projectId
	return &bigqueryGithubDataSink{
		opts:          opts,
		clientContext: context.Background(),
		cancelFuncs:   make([]context.CancelFunc, 0),
	}
}

type bigqueryGithubDataSink struct {
	opts        BigqueryDataSinkOptions
	initialized bool

	clientContext context.Context
	client        *bigquery.Client
	cancelFuncs   []context.CancelFunc

	mux sync.RWMutex
}

func (b *bigqueryGithubDataSink) Init() error {
	b.mux.Lock()
	defer b.mux.Unlock()

	// Note: dialing is nonblocking and timeouts are not recommended when
	// instantiating a client as it may interfere with token refreshing
	client, err := bigquery.NewClient(b.clientContext, b.opts.ProjectId)
	if err != nil {
		return err
	}

	b.client = client
	b.initialized = true

	return nil
}

func (b *bigqueryGithubDataSink) Close() error {
	if !b.isInitialized() {
		return ErrDatasinkNotInitialized{}
	}

	b.mux.Lock()
	b.initialized = false
	cancelFuncs := b.cancelFuncs
	b.mux.Unlock()

	for _, cancelFunc := range cancelFuncs {
		cancelFunc()
	}
	b.client.Close()

	return nil
}

func (b *bigqueryGithubDataSink) FetchTopNRepositories(n int, eventType GithubEventType) (chan GithubRepository, context.CancelFunc, error) {
	outputCh := make(chan GithubRepository, b.opts.ResultsChannelBufferSize)

	query := bigqueryGithubTopNRepositoriesQuery(b.client, eventType, n)
	cancelFunc, err := b.executeQuery(query, func(row []bigquery.Value, err error) error {
		if err == iterator.Done {
			close(outputCh)
			return nil
		}

		repoId, err := parseBigqueryInt(row[0], "repoId")
		if err != nil {
			outputCh <- GithubRepository{
				Err: err,
			}
			return err
		}

		repoUrl, err := parseBigqueryString(row[1], "repoUrl")
		if err != nil {
			outputCh <- GithubRepository{
				Err: err,
			}
			return nil
		}

		eventCount, err := parseBigqueryInt(row[2], "eventCount")
		if err != nil {
			outputCh <- GithubRepository{
				Err: err,
			}
			return nil
		}

		rank, err := parseBigqueryInt(row[3], "rank")
		if err != nil {
			outputCh <- GithubRepository{
				Err: err,
			}
			return nil
		}

		outputCh <- GithubRepository{
			Err:           nil,
			RepositoryId:  repoId,
			RepositoryUrl: repoUrl,
			EventCount:    eventCount,
			Rank:          rank,
		}

		return nil
	})

	return outputCh, cancelFunc, err
}

func (b *bigqueryGithubDataSink) FetchEventCounts(repositories []GithubRepository, eventType GithubEventType, startTs, endTs time.Time) (chan GithubRepositoryEventDayBucket, context.CancelFunc, error) {
	outputCh := make(chan GithubRepositoryEventDayBucket, b.opts.ResultsChannelBufferSize)

	query := bigqueryGithubEventCountsQuery(b.client, eventType, repositories, startTs, endTs)
	cancelFunc, err := b.executeQuery(query, func(row []bigquery.Value, err error) error {
		if err == iterator.Done {
			close(outputCh)
			return nil
		}

		repoId, err := parseBigqueryInt(row[0], "repoId")
		if err != nil {
			outputCh <- GithubRepositoryEventDayBucket{
				Err: err,
			}
			return nil
		}

		dayBucketTs, err := parseBigqueryTimestamp(row[1], "dayBucket")
		if err != nil {
			outputCh <- GithubRepositoryEventDayBucket{
				Err: err,
			}
			return nil
		}

		eventCount, err := parseBigqueryInt(row[2], "eventCount")
		if err != nil {
			outputCh <- GithubRepositoryEventDayBucket{
				Err: err,
			}
			return nil
		}

		outputCh <- GithubRepositoryEventDayBucket{
			Err:          nil,
			RepositoryId: repoId,
			Count:        eventCount,
			DayBucketTs:  dayBucketTs,
		}

		return nil
	})

	return outputCh, cancelFunc, err

}

func (b *bigqueryGithubDataSink) executeQuery(query *bigquery.Query, cb func([]bigquery.Value, error) error) (context.CancelFunc, error) {
	if !b.isInitialized() {
		return nil, ErrDatasinkNotInitialized{}
	}

	transactionContext, cancelFunc := context.WithTimeout(b.clientContext, b.opts.ReadTransactionTimeout)
	rowIterator, err := query.Read(transactionContext)
	if err != nil {
		return cancelFunc, err
	}
	b.addCancelFunc(cancelFunc)

	go func(it *bigquery.RowIterator) {
		defer cancelFunc()

		for {
			var values []bigquery.Value
			err := it.Next(&values)

			if cbErr := cb(values, err); cbErr != nil {
				return
			}

			if err == iterator.Done {
				return
			}

		}
	}(rowIterator)

	return cancelFunc, nil
}

func (b *bigqueryGithubDataSink) isInitialized() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()

	return b.initialized
}

func (b *bigqueryGithubDataSink) addCancelFunc(fn context.CancelFunc) {
	b.mux.Lock()
	defer b.mux.Unlock()

	b.cancelFuncs = append(b.cancelFuncs, fn)
}

func parseBigqueryInt(value bigquery.Value, name string) (int, error) {
	parsed, ok := value.(int64)
	if !ok {
		return 0, ErrUnableToParseField{name}
	}

	return int(parsed), nil
}

func parseBigqueryString(value bigquery.Value, name string) (string, error) {
	parsed, ok := value.(string)
	if !ok {
		return "", ErrUnableToParseField{name}
	}

	return parsed, nil
}

func parseBigqueryTimestamp(value bigquery.Value, name string) (time.Time, error) {
	parsed, ok := value.(time.Time)
	if !ok {
		return time.Time{}, ErrUnableToParseField{name}
	}

	return parsed, nil
}
