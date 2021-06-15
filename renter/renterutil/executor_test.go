package renterutil_test

import (
	"context"
	"errors"
	"testing"

	"lukechampine.com/us/renter/renterutil"
)

func TestDefaultExecutor(t *testing.T) {
	e := errors.New("expected error")
	parentCtx := context.WithValue(context.Background(), "key", "value")

	err := renterutil.DefaultExecutor(parentCtx, func(ctx context.Context) error {
		if ctx != parentCtx {
			t.Errorf("expect %v, got %v", parentCtx, ctx)
		}
		return e
	})
	if err != e {
		t.Errorf("expect %v, got %v", e, err)
	}
}
