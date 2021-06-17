package renterutil

import (
	"context"
	"reflect"
	"testing"
	"time"

	"lukechampine.com/us/renter/proto"
)

func TestNewRPCStatsRecorder(t *testing.T) {
	t.Run("with a recorder", func(t *testing.T) {
		parentCtx := context.WithValue(context.Background(), "key", "value")
		expect := proto.RPCStats{
			Timestamp: time.Now(),
		}

		r := newRPCStatsRecorder(parentCtx, RPCStatsRecorderFunc(func(ctx context.Context, stats *proto.RPCStats) {
			if ctx != parentCtx {
				t.Errorf("expect %v, got %v", parentCtx, ctx)
			}
			if !reflect.DeepEqual(stats, &expect) {
				t.Errorf("expect %v, got %v", &expect, stats)
			}
		}))
		r.RecordRPCStats(expect)
	})

	t.Run("nil recorder", func(t *testing.T) {
		r := newRPCStatsRecorder(context.Background(), nil)
		if r != nil {
			t.Error("expect nil")
		}
	})
}
