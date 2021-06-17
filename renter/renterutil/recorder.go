package renterutil

import (
	"context"

	"lukechampine.com/us/renter/proto"
)

// RPCStatsRecorder extends proto.RPCStatsRecorder so that it can receive a context.
type RPCStatsRecorder interface {
	RecordRPCStats(ctx context.Context, stats *proto.RPCStats)
}

type RPCStatsRecorderFunc func(context.Context, *proto.RPCStats)

func (f RPCStatsRecorderFunc) RecordRPCStats(ctx context.Context, stats *proto.RPCStats) {
	f(ctx, stats)
}

// newRPCStatsRecorder creates an adapter of RPCStatsRecorder to use it as proto.RPCStatsRecorder.
func newRPCStatsRecorder(ctx context.Context, r RPCStatsRecorder) proto.RPCStatsRecorder {
	if r == nil {
		return nil
	}
	return proto.RPCStatsRecorderFunc(func(stats proto.RPCStats) { r.RecordRPCStats(ctx, &stats) })
}
