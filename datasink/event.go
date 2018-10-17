package datasink

type GithubEventType string

const (
	WatchGithubEventType              GithubEventType = "WatchEvent"
	PullRequestGithubEventType                        = "PullRequestEvent"
	PullRequestCommentGithubEventType                 = "PullRequestReviewCommentEvent"
	IssueGithubEventType                              = "IssuesEvent"
	IssueCommentGithubEventType                       = "IssueCommentEvent"

	NilGithubEventType = ""
)

func ParseGithubEventType(input string) (GithubEventType, error) {
	event, ok := map[string]GithubEventType{
		"watches":               WatchGithubEventType,
		"pull-requests":         PullRequestGithubEventType,
		"pull-request-comments": PullRequestCommentGithubEventType,
		"issues":                IssueGithubEventType,
		"issue-comments":        IssueCommentGithubEventType,
	}[input]

	if !ok {
		return NilGithubEventType, ErrInvalidGithubEventType{input}
	}

	return event, nil
}
