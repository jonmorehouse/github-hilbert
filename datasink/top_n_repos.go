package datasink

// FetchTopNGithubRepositories: synchronously fetch the topN repositories based
// on the specified event type
func FetchTopNGithubRepositories(dataSink GithubDataSink, n int, eventType GithubEventType) ([]GithubRepository, error) {
	if eventType == NilGithubEventType {
		return []GithubRepository(nil), ErrInvalidGithubEventType{"nil"}
	}

	recvCh, cancelFunc, err := dataSink.FetchTopNRepositories(n, eventType)
	if err != nil {
		return []GithubRepository(nil), err
	}
	defer cancelFunc()

	repos := make([]GithubRepository, 0)
	for repoData := range recvCh {
		if repoData.Err != nil {
			return nil, repoData.Err
		}

		repos = append(repos, repoData)
	}

	if len(repos) == 0 {
		return []GithubRepository(nil), ErrNoRepositoriesFound{eventType}
	}

	return repos, nil
}
