package main

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLeadershipGateRejectsSchedulerRPCWhileFollower(t *testing.T) {
	gate := newLeadershipGate(false)
	handlerCalled := false
	handler := func(context.Context, any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	_, err := gate.unaryServerInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/agentenv.scheduler.v1.Scheduler/Schedule"}, handler)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected follower RPC to be unavailable, got %v", err)
	}
	if handlerCalled {
		t.Fatal("follower invoked scheduler handler")
	}
}

func TestLeadershipGateAllowsHealthAndLeaderRPCs(t *testing.T) {
	gate := newLeadershipGate(false)
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	response, err := gate.unaryServerInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, handler)
	if err != nil || response != "ok" {
		t.Fatalf("health RPC should bypass leadership gate: response=%v error=%v", response, err)
	}

	gate.setLeader(true)
	response, err = gate.unaryServerInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/agentenv.scheduler.v1.Scheduler/Schedule"}, handler)
	if err != nil || response != "ok" {
		t.Fatalf("leader RPC should pass: response=%v error=%v", response, err)
	}
}
