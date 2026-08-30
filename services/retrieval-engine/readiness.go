package main

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

const dependencyCheckTimeout = 1500 * time.Millisecond

type dependencyCheck func(context.Context) error

func checkDependencies(ctx context.Context, checks ...dependencyCheck) error {
	if len(checks) == 0 {
		return fmt.Errorf("retrieval readiness: at least one dependency check is required")
	}
	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(checks))
	for _, check := range checks {
		if check == nil {
			return fmt.Errorf("retrieval readiness: dependency check is nil")
		}
		go func(check dependencyCheck) {
			results <- check(checkCtx)
		}(check)
	}
	for range checks {
		select {
		case err := <-results:
			if err != nil {
				return err
			}
		case <-checkCtx.Done():
			return checkCtx.Err()
		}
	}
	return nil
}

func newDependencyReadinessHandler(checks ...dependencyCheck) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), dependencyCheckTimeout)
		defer cancel()
		response.Header().Set("Cache-Control", "no-store")
		if checkDependencies(ctx, checks...) != nil {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	})
}
