package observability

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Keep in sync with src/prometheus.rs.
var DurationBuckets = []float64{
	0.001,
	0.002,
	0.005,
	0.010,
	0.025,
	0.050,
	0.100,
	0.250,
	0.500,
	1.0,
	2.5,
	5.0,
	10.0,
	30.0,
	60.0,
	120.0,
	300.0,
	600.0,
	900.0,
	1200.0,
	1800.0,
}

func GRPCStatusLabel(err error) string {
	if err == nil {
		return "ok"
	}
	switch status.Code(err) {
	case codes.OK:
		return "ok"
	case codes.Canceled:
		return "canceled"
	case codes.InvalidArgument:
		return "invalid_argument"
	case codes.NotFound:
		return "not_found"
	case codes.FailedPrecondition:
		return "failed_precondition"
	case codes.Unavailable:
		return "unavailable"
	case codes.DeadlineExceeded:
		return "deadline_exceeded"
	case codes.Internal:
		return "internal"
	default:
		return "other"
	}
}
