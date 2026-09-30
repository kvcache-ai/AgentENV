package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"google.golang.org/grpc"
)

type fleetStatusServer struct {
	schedulerv1.UnimplementedSchedulerServer
}

func (fleetStatusServer) ListObservedNodes(context.Context, *schedulerv1.ListObservedNodesRequest) (*schedulerv1.ListObservedNodesResponse, error) {
	return &schedulerv1.ListObservedNodesResponse{
		Nodes: []*schedulerv1.ObservedNode{{NodeId: strings.Repeat("x", 5<<20)}},
	}, nil
}

func TestSchedulerConnectionReceivesFleetStatusOverFourMiB(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	schedulerv1.RegisterSchedulerServer(server, fleetStatusServer{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := newSchedulerConn(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := schedulerv1.NewSchedulerClient(conn).ListObservedNodes(ctx, &schedulerv1.ListObservedNodesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(response.GetNodes()[0].GetNodeId()); got != 5<<20 {
		t.Fatalf("response length = %d, want %d", got, 5<<20)
	}
}
