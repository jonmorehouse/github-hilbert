package datasink

import "fmt"

// This module contains common errors emitted from interfaces and methods in
// this package

type ErrDatasinkNotInitialized struct{}

func (e ErrDatasinkNotInitialized) Error() string {
	return "datasink not initialized"
}

type ErrInvalidBucketType struct {
	bucketType string
}

func (i ErrInvalidBucketType) Error() string {
	return "invalid bucket-type: " + i.bucketType
}

type ErrInvalidDateStr struct {
	desc string
}

func (i ErrInvalidDateStr) Error() string {
	return fmt.Sprintf("invalid date str %s:", i.desc)
}

type ErrInvalidGithubEventType struct {
	event string
}

func (i ErrInvalidGithubEventType) Error() string {
	return fmt.Sprintf("%s not one of %s", i.event, []GithubEventType{WatchGithubEventType,
		PullRequestGithubEventType,
		PullRequestCommentGithubEventType,
		IssueGithubEventType,
		IssueCommentGithubEventType})

}

type ErrNoRepositoriesFound struct {
	githubEventType GithubEventType
}

func (e ErrNoRepositoriesFound) Error() string {
	return fmt.Sprintf("no repositories found with github event type %s", e.githubEventType)
}

type ErrUnableToParseField struct {
	name string
}

func (d ErrUnableToParseField) Error() string { return fmt.Sprintf("unable to parse field %s", d.name) }
