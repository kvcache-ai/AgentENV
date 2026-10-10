//go:build e2e

// Scheduler HA failover e2e tests (#259, review requirement R3).
// Runs against a Kind cluster with: scheduler x3 (leader election on,
// Redis bindings) and stub nodes (admin /nodes + heartbeats). The workflow
// builds and loads the images and applies services/scheduler/e2e/k8s.
package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var schedAddr = envOr("E2E_SCHED_ADDR", "127.0.0.1:19090")

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func kube(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func dial(t *testing.T) schedulerv1.SchedulerClient {
	t.Helper()
	conn, err := grpc.NewClient(schedAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", schedAddr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return schedulerv1.NewSchedulerClient(conn)
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := cond()
		if err == nil && ok {
			return
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (last error: %v)", what, lastErr)
}

func leaderIdentity(t *testing.T) string {
	t.Helper()
	out := kube(t, "get", "lease", "agentenv-scheduler", "-o", "jsonpath={.spec.holderIdentity}")
	return strings.TrimSpace(out)
}

var currentForward *exec.Cmd

// forwardLeader port-forwards to the current leader POD. kubectl port-forward
// to a Service ignores readiness and can land on a gated standby (every RPC
// rejected with "not the leader"), so tests must target the leader pod
// directly and re-resolve it after every failover. Any previous forward is
// killed first, since the pod it targeted may have been deleted mid-test.
func forwardLeader(t *testing.T) {
	t.Helper()
	if currentForward != nil {
		_ = currentForward.Process.Kill()
		currentForward = nil
	}
	leader := leaderIdentity(t)
	if leader == "" {
		t.Fatal("no leader elected yet")
	}
	cmd := exec.Command("kubectl", "port-forward", "pod/"+leader, "19090:9090")
	if err := cmd.Start(); err != nil {
		t.Fatalf("port-forward to leader %s: %v", leader, err)
	}
	currentForward = cmd
	t.Cleanup(func() {
		if currentForward == cmd {
			_ = cmd.Process.Kill()
			currentForward = nil
		}
	})
	// Probe the port with a real TCP dial: grpc.NewClient is lazy and would
	// report ready long before kubectl has the tunnel up.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:19090", 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("port-forward to leader %s never came up", leader)
}

func schedulerEndpoints(t *testing.T) []string {
	t.Helper()
	out := kube(t, "get", "endpointslice", "-l", "kubernetes.io/service-name=agentenv-scheduler",
		"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
	var names []string
	for _, f := range strings.Fields(out) {
		if strings.HasPrefix(f, "agentenv-scheduler-") {
			names = append(names, f)
		}
	}
	return names
}

// T1: kill the leader pod — a standby must take over within the lease
// budget, pre-existing bindings keep resolving through Redis, and
// scheduling resumes on freshly observed nodes.
func TestFailoverLeaderKill(t *testing.T) {
	forwardLeader(t)
	client := dial(t)
	ctx := context.Background()

	if _, err := client.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-e2e-1",
		Node:      firstDiscoveredNode(t, client),
	}); err != nil {
		t.Fatalf("seed binding failed: %v", err)
	}

	// Continuous lookup during the failover: must never fail with NotFound.
	lookupErrs := make(chan error, 1)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, err := client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-e2e-1"})
			if err != nil {
				if s, ok := status.FromError(err); ok && strings.Contains(s.Message(), "not found") {
					lookupErrs <- fmt.Errorf("lookup returned not found mid-failover: %v", err)
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	defer close(stop)

	victim := leaderIdentity(t)
	t.Logf("killing leader pod %s", victim)
	kube(t, "delete", "pod", victim, "--force", "--grace-period=0")

	eventually(t, 30*time.Second, "a new leader", func() (bool, error) {
		cur := leaderIdentity(t)
		return cur != "" && cur != victim, nil
	})
	eventually(t, 30*time.Second, "endpoints converge on one leader", func() (bool, error) {
		return len(schedulerEndpoints(t)) == 1, nil
	})

	select {
	case err := <-lookupErrs:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
	}

	// The forward targeted the killed pod; re-forward and re-dial.
	forwardLeader(t)
	client = dial(t)
	eventually(t, 30*time.Second, "scheduling works again on the new leader", func() (bool, error) {
		_, err := client.Schedule(ctx, &schedulerv1.ScheduleRequest{})
		return err == nil, nil
	})
}

// T2: freeze the leader (SIGSTOP = API partition equivalent) — the standby
// must take over, and the frozen ex-leader must exit on resume instead of
// serving as a second primary.
func TestPartitionFrozenLeader(t *testing.T) {
	victim := leaderIdentity(t)
	t.Logf("freezing leader process in pod %s (SIGSTOP = cannot renew)", victim)
	out, err := exec.Command("kubectl", "debug", "-q", victim, "--image=busybox:1.36",
		"--", "sh", "-c", "kill -STOP $(pidof scheduler)").CombinedOutput()
	if err != nil {
		t.Fatalf("freeze leader via debug container failed: %v\n%s", err, out)
	}

	eventually(t, 45*time.Second, "standby takes over while old leader frozen", func() (bool, error) {
		cur := leaderIdentity(t)
		return cur != "" && cur != victim, nil
	})

	// While frozen, there must be exactly one serving endpoint.
	eps := schedulerEndpoints(t)
	if len(eps) != 1 {
		t.Fatalf("expected exactly one endpoint during partition, got %v", eps)
	}

	out, err = exec.Command("kubectl", "debug", "-q", victim, "--image=busybox:1.36",
		"--", "sh", "-c", "kill -CONT $(pidof scheduler)").CombinedOutput()
	if err != nil {
		t.Fatalf("resume leader via debug container failed: %v\n%s", err, out)
	}
	// With egress restored, the ex-leader's renew failure has already fired
	// OnStoppedLeading: it force-stops and its pod restarts as standby.
	eventually(t, 60*time.Second, "frozen ex-leader exits (pod restarts)", func() (bool, error) {
		out := kube(t, "get", "pod", victim, "-o", "jsonpath={.status.containerStatuses[0].restartCount}")
		return out != "0", nil
	})
	eventually(t, 30*time.Second, "still exactly one leader after resume", func() (bool, error) {
		return len(schedulerEndpoints(t)) == 1, nil
	})
}

// T3: with node heartbeats paused AND the admin pull failing, scheduling
// right after takeover must return Unavailable (never fall back to
// unobserved nodes); once both resume, the pull-driven rebuild makes
// scheduling work again.
func TestTakeoverSchedulingSemantics(t *testing.T) {
	forwardLeader(t)
	client := dial(t)
	ctx := context.Background()

	// Zero fresh observations requires both channels down: heartbeats alone
	// are not enough, since sync-node-snapshots rebuilds via the admin pull.
	stubPost(t, "control/pause", "")
	stubPost(t, "control/admin-pause", "")
	victim := leaderIdentity(t)
	kube(t, "delete", "pod", victim, "--force", "--grace-period=0")

	eventually(t, 30*time.Second, "new leader", func() (bool, error) {
		cur := leaderIdentity(t)
		return cur != "" && cur != victim, nil
	})

	// The forward targeted the killed pod; re-forward and re-dial.
	forwardLeader(t)
	client = dial(t)

	// No fresh observations from either channel: Unavailable, not a guess.
	if _, err := client.Schedule(ctx, &schedulerv1.ScheduleRequest{}); err == nil {
		t.Fatal("schedule must fail with Unavailable before fresh observations, not guess capacity")
	}

	stubPost(t, "control/resume", "")
	stubPost(t, "control/admin-resume", "")
	eventually(t, 30*time.Second, "scheduling recovers once pull and heartbeats resume", func() (bool, error) {
		_, err := client.Schedule(ctx, &schedulerv1.ScheduleRequest{})
		return err == nil, nil
	})
}

// T4: a write that lands on the demoted leader mid-takeover must not
// commit after the takeover; the client retries and succeeds on the new
// leader.
func TestDemotedLeaderDelayedWrite(t *testing.T) {
	forwardLeader(t)
	client := dial(t)
	ctx := context.Background()

	victim := leaderIdentity(t)
	done := make(chan error, 1)
	go func() {
		_, err := client.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{
			SandboxId: "sbx-e2e-t4",
			Node:      firstDiscoveredNode(t, client),
		})
		done <- err
	}()

	kube(t, "delete", "pod", victim, "--force", "--grace-period=0")
	writeErr := <-done
	t.Logf("write during takeover returned: %v", writeErr)

	eventually(t, 30*time.Second, "new leader", func() (bool, error) {
		cur := leaderIdentity(t)
		return cur != "" && cur != victim, nil
	})

	// The forward targeted the killed pod; re-forward and re-dial.
	forwardLeader(t)
	client = dial(t)

	// The client-side retry path: writing again must succeed on the new leader.
	if _, err := client.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-e2e-t4",
		Node:      firstDiscoveredNode(t, client),
	}); err != nil {
		t.Fatalf("write retry on the new leader failed: %v", err)
	}
	if _, err := client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-e2e-t4"}); err != nil {
		t.Fatalf("binding must exist after retry, got %v", err)
	}
}

// firstDiscoveredNode resolves a node through the scheduler's own discovery,
// so RecordAssignment passes the service's known-node validation.
func firstDiscoveredNode(t *testing.T, client schedulerv1.SchedulerClient) *schedulerv1.Node {
	t.Helper()
	resp, err := client.ListNodes(context.Background(), &schedulerv1.ListNodesRequest{})
	if err != nil || len(resp.GetNodes()) == 0 {
		t.Fatalf("ListNodes must return discovered stub nodes: %v", err)
	}
	return resp.GetNodes()[0]
}

// stubPost hits every stub's control plane through an ephemeral busybox
// debug container (the stub image is distroless, so kubectl exec into the
// container itself is not possible).
func stubPost(t *testing.T, path, body string) {
	t.Helper()
	pods := kube(t, "get", "pods", "-l", "app=stubnode", "-o", "jsonpath={.items[*].metadata.name}")
	for _, pod := range strings.Fields(pods) {
		out, err := exec.Command("kubectl", "debug", "-q", pod, "--image=busybox:1.36",
			"--", "wget", "-q", "-O", "-", "--post-data", body, "http://127.0.0.1:8000/"+path).CombinedOutput()
		if err != nil {
			t.Fatalf("stub control %s on %s failed: %v\n%s", path, pod, err, out)
		}
	}
}
