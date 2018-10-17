package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jonmorehouse/github-hilbert/datasink"
	"github.com/jonmorehouse/github-hilbert/hilbert"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

type invalidArgErr struct {
	flags, desc string
}

func (i invalidArgErr) Error() string {
	return fmt.Sprintf("invalid flags %s  %s", i.flags, i.desc)
}

type options struct {
	gcpProjectId   string
	startTs, endTs time.Time

	popularRepositoriesCount                            int
	popularRepositoriesGithubEventType, githubEventType datasink.GithubEventType

	bucketType datasink.BucketType

	minRefreshDeadline time.Duration
	hilbertPeanoFactor int
}

// newOptions: create and parse flags, returning an options struct with all values
func newOptions() (options, error) {
	// gcp options
	gcpProjectId := flag.String("gcp-project-id", "", "project id to be used for BigQuery access")

	// datasink options
	startDate := flag.String("start-date", "20150101", "YYYYMMDD start date; defaults to 20150101")
	endDate := flag.String("end-date", "", "YYYYMMDD end date; defaults to today")
	popularRepositoriesCount := flag.Int("popular-repositories-count", 1000, "n count of most popular repositories by stars; default 1000")
	popularRepositoriesEventType := flag.String("popular-repositories-event-type", "watches", "github event type to sort most popular repositories by; default watches")
	eventType := flag.String("event-type", "watches", "github event type to visualize; default watches")

	// hilbert options
	minRefreshDeadline := flag.String("min-refresh-deadline", "1s", "Minimum time between refreshes of the visualization")
	hilbertPeanoFactor := flag.Int("hilbert-peano-factor", 5, "peano factor for hilbert visualization")

	flag.Parse()
	opts := options{}

	if *gcpProjectId == "" {
		return options{}, invalidArgErr{"-gcp-project-id", "field is required"}
	}
	opts.gcpProjectId = *gcpProjectId

	if *popularRepositoriesCount < 1 {
		return options{}, invalidArgErr{"-popular-repositories-count", "-popular-repositories-count must be > 0"}
	}
	opts.popularRepositoriesCount = *popularRepositoriesCount

	parsedPopularRepositoriesGithubEventType, err := datasink.ParseGithubEventType(*popularRepositoriesEventType)
	if err != nil {
		return options{}, invalidArgErr{"-popular-repositories-event-type", err.Error()}
	}
	opts.popularRepositoriesGithubEventType = parsedPopularRepositoriesGithubEventType

	parsedEventType, err := datasink.ParseGithubEventType(*eventType)
	if err != nil {
		return options{}, invalidArgErr{"-event-type", err.Error()}
	}
	opts.githubEventType = parsedEventType

	startTs, err := datasink.ParseDateString(*startDate)
	if err != nil {
		return options{}, invalidArgErr{"-start-date", err.Error()}
	}

	endTs := time.Now()
	if *endDate != "" {
		endTs, err = datasink.ParseDateString(*endDate)
		if err != nil {
			return options{}, invalidArgErr{"-end-date", err.Error()}
		}
	}
	opts.endTs = endTs

	if startTs.After(endTs) {
		return options{}, invalidArgErr{"-start-date,-end-date", "start-date can not be after end date"}
	}

	duration, err := time.ParseDuration(*minRefreshDeadline)
	if err != nil {
		return options{}, invalidArgErr{"-min-refresh-deadline", err.Error()}
	}
	opts.minRefreshDeadline = duration

	if *hilbertPeanoFactor < 1 {
		return options{}, invalidArgErr{"-hilbert-peano-factor", "must be greater than 1"}
	}
	opts.hilbertPeanoFactor = *hilbertPeanoFactor

	return opts, nil
}

// run: initializes datasink, bucketer and displayer. Returns a `doneCh` which
// is used to denote when all resources have be closed and execution is complete
func run(opts options) (chan struct{}, func(), error) {
	doneCh := make(chan struct{})

	dataSink := datasink.NewBigqueryGithubDataSink(opts.gcpProjectId, datasink.BigqueryDataSinkOptions{})
	if err := dataSink.Init(); err != nil {
		return nil, nil, err
	}

	if err := dataSink.Init(); err != nil {
		return nil, nil, err
	}

	repositories, err := datasink.FetchTopNGithubRepositories(dataSink, opts.popularRepositoriesCount, opts.popularRepositoriesGithubEventType)
	if err != nil {
		return nil, nil, err
	}

	eventCh, cancelFn, err := dataSink.FetchEventCounts(repositories, opts.githubEventType, opts.startTs, opts.endTs)
	if err != nil {
		return nil, nil, err
	}

	totalDays := int(opts.endTs.Sub(opts.startTs).Hours()) / 24
	aggregator := datasink.NewEventDayBucketAggregator(eventCh, datasink.EventDayBucketAggregatorOptions{
		BufferSize: totalDays,
		BucketType: opts.bucketType,
	})
	aggregator.Init()

	stop := func() {
		var once sync.Once
		once.Do(func() {
			cancelFn()
			dataSink.Close()
			aggregator.Close()
			close(doneCh)
		})
	}

	go func() {
		for dayBucketEvent := range aggregator.OutputCh() {
			log.Println("received")
			log.Println(dayBucketEvent.DayBucketTs)
		}
		stop()
	}()

	return doneCh, stop, nil
}

func runDisplay(opts options) (chan struct{}, func(), error) {
	hilbertOpts := hilbert.NewDefaultTerminalDisplayOptions()
	hilbertOpts.MinRefresh = opts.minRefreshDeadline
	hilbertOpts.PeanoFactor = opts.hilbertPeanoFactor

	display := hilbert.NewTerminalDisplay(hilbertOpts)

	if err := display.Init(); err != nil {
		return nil, nil, err
	}

	doneCh := make(chan struct{})

	go func() {
		for err := range display.ErrCh() {
			_, ok := err.(hilbert.ErrDisplayInterrupt)
			if ok {
				break
			}
		}
		display.Close()
		close(doneCh)
	}()

	// TEMP REMOVE THIS!
	go func() {
		// TODO
		go func() {
			<-time.After(time.Second * 5)
			display.Close()
		}()

	}()

	return doneCh, func() {}, nil
}

// main: fetch activity (commits, prs, lines added/deleted) of the top N
// repositories by activity and build a hilbert curve of the distribution of
// activities over the time period from start-date -> end-date (in day buckets)
func main() {
	opts, err := newOptions()
	if err != nil {
		log.Fatal(err)
	}

	//doneCh, stopFn, err := run(opts)
	doneCh, stopFn, err := runDisplay(opts)
	if err != nil {
		log.Fatal(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	select {
	case <-doneCh:
	case <-sigCh:
		log.Println("signal received ...")
		stopFn()
	}
	close(sigCh)
}
