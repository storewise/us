package renterutil

import "context"

// RequestExecutor provides how to execute a request, which is enclosed in the given function.
type RequestExecutor interface {
	// Execute executes the given request and returns an error if fails.
	Execute(context.Context, func(context.Context) error) error
}

// RequestExecutorFunc is an adapter that allows to use a function as a RequestExecutor.
type RequestExecutorFunc func(context.Context, func(context.Context) error) error

var _ RequestExecutor = (RequestExecutorFunc)(nil)

// Execute executes the given request and returns an error if fails.
func (f RequestExecutorFunc) Execute(ctx context.Context, req func(context.Context) error) error {
	return f(ctx, req)
}

// DefaultExecutor executes the given request as is.
var DefaultExecutor RequestExecutorFunc = func(ctx context.Context, req func(context.Context) error) error {
	return req(ctx)
}
