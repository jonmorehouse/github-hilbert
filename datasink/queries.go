package datasink

import (
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
)

// This module holds functions for generating BigQuery SQL queries. Each
// queries uses the bigquery standard syntax:
// https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
//
// The standard syntax uses ` (backtick) characters for delimiting table
// locations. Backticks are not compatible with uninterpretted strings in Go
// and have been replaced with the | (pipe) character.

// bigqueryGithubTopNRepositoriesQuery: find top N repositories based on a
// given event type.
func bigqueryGithubTopNRepositoriesQuery(client *bigquery.Client, eventType GithubEventType, n int) *bigquery.Query {
	query := client.Query(bigqueryGithubTopNRepositoriesQueryTmpl())
	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "n",
			Value: n,
		},
		{
			Name:  "event_type",
			Value: string(eventType),
		},
	}

	return query
}

func bigqueryGithubTopNRepositoriesQueryTmpl() string {
	tmpl := `
WITH
  event_counts AS (
    SELECT COUNT(type) AS event_count,
      repo.id AS repo_id,
      MAX(repo.url) AS repo_url
    FROM |githubarchive.month.201802*|
    WHERE type=@event_type
      AND repo.id IS NOT NULL
    GROUP BY repo.id
  ),
  ranked_repositories AS (
    SELECT event_count, repo_id, repo_url,
      RANK() OVER ( PARTITION BY MOD(repo_id, 10) ORDER BY event_count DESC ) as rank
    FROM event_counts
  )

SELECT repo_id, repo_url, event_count, ROW_NUMBER() OVER ( ORDER BY event_count DESC ) as rank
FROM ranked_repositories
  WHERE rank < @n
  LIMIT @n;
`
	return strings.Replace(tmpl, "|", "`", 2)
}

// bigqueryGithubEventCounts: fetch day bucket resolution counts for
// the given event across the given repositores.
func bigqueryGithubEventCountsQuery(client *bigquery.Client, eventType GithubEventType, repositories []GithubRepository, startTs, endTs time.Time) *bigquery.Query {
	query := client.Query(bigqueryGithubEventCountsQueryTmpl())

	repoIds := make([]int64, len(repositories))
	for idx, repo := range repositories {
		repoIds[idx] = int64(repo.RepositoryId)
	}

	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "event_type",
			Value: string(eventType),
		},
		{
			Name:  "start_ts",
			Value: int64(startTs.Unix()),
		},
		{
			Name:  "end_ts",
			Value: int64(endTs.Unix()),
		},
		{
			Name:  "repo_ids",
			Value: repoIds,
		},
	}

	return query
}

func bigqueryGithubEventCountsQueryTmpl() string {
	tmpl := `
WITH
  day_bucket_counts AS (
    SELECT repo.id AS repo_id, COUNT(type) AS event_count, EXTRACT(DATE FROM created_at) AS day_bucket
    FROM |githubarchive.month.201802|
    WHERE type=@event_type
      AND created_at > TIMESTAMP_SECONDS(@start_ts)
      AND created_at < TIMESTAMP_SECONDS(@end_ts)
      AND repo.id IN UNNEST(@repo_ids)
    GROUP BY repo.id, day_bucket
  ),

  day_bucket_ranks AS (
    SELECT repo_id, day_bucket, event_count,
      RANK() OVER ( PARTITION BY day_bucket ORDER BY event_count DESC ) as rank
    FROM day_bucket_counts
  )

SELECT repo_id, CAST(day_bucket AS TIMESTAMP) as day_bucket, event_count
  FROM day_bucket_ranks
  WHERE rank < 1000
  ORDER BY day_bucket, event_count ASC
`
	return strings.Replace(tmpl, "|", "`", 2)
}
