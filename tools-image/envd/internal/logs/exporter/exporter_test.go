package exporter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/e2b-dev/infra/packages/envd/internal/host"
)

func TestQueueWithoutCollector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewHTTPLogsExporter(ctx, false, make(chan *host.MMDSOpts))
	data := make([]byte, 64*1024)
	for i := 0; i < 1024; i++ {
		n, err := w.Write(data)
		if n != len(data) || err != nil {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
	if w.queuedLogBytes != maxQueuedLogBytes {
		t.Fatalf("queued bytes = %d", w.queuedLogBytes)
	}
	t.Logf("64MiB input: retained %d bytes in %d records", w.queuedLogBytes, len(w.logs))
}

func TestQueueRecordBoundAndReuse(t *testing.T) {
	w := &HTTPExporter{triggers: make(chan struct{}, 1)}
	for i := 0; i < 2*maxQueuedLogRecords; i++ {
		w.Write([]byte("x"))
	}
	if len(w.logs) != maxQueuedLogRecords {
		t.Fatalf("records = %d", len(w.logs))
	}
	batch := w.getAllLogs()
	if len(batch) != maxQueuedLogRecords || w.queuedLogBytes != 0 || len(w.logs) != 0 {
		t.Fatal("drain did not reset queue")
	}
	w.Write([]byte("next"))
	if w.queuedLogBytes != 4 || len(w.logs) != 1 {
		t.Fatal("queue did not accept after drain")
	}
}

func TestQueueCopiesAndOrdersRecords(t *testing.T) {
	w := &HTTPExporter{triggers: make(chan struct{}, 1)}
	data := []byte("first")
	w.Write(data)
	copy(data, "other")
	w.Write([]byte("second"))
	batch := w.getAllLogs()
	if string(batch[0]) != "first" || string(batch[1]) != "second" {
		t.Fatalf("records = %q", batch)
	}
}

func TestOversizedTelemetryDoesNotBlockLocalOutput(t *testing.T) {
	w := &HTTPExporter{triggers: make(chan struct{}, 1)}
	data := make([]byte, maxQueuedLogBytes+1)
	var local bytes.Buffer
	n, err := io.MultiWriter(w, &local).Write(data)
	if !bytes.Equal(local.Bytes(), data) {
		t.Fatal("local output changed")
	}
	if n != len(data) || err != nil || len(w.logs) != 0 || w.queuedLogBytes != 0 {
		t.Fatal("oversized record retained or short-written")
	}
	w.Write([]byte("next"))
	if string(w.getAllLogs()[0]) != "next" {
		t.Fatal("oversized record blocked later telemetry")
	}
}

func TestConcurrentQueueBound(t *testing.T) {
	w := &HTTPExporter{triggers: make(chan struct{}, 1)}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 64*1024)
			for j := 0; j < 128; j++ {
				w.Write(data)
			}
		}()
	}
	wg.Wait()
	total := 0
	for _, data := range w.logs {
		total += len(data)
	}
	if total > maxQueuedLogBytes || total != w.queuedLogBytes || len(w.logs) > maxQueuedLogRecords {
		t.Fatalf("bytes=%d records=%d", total, len(w.logs))
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSlowCollectorQueueBound(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	w := &HTTPExporter{
		triggers: make(chan struct{}, 1),
		mmdsOpts: &host.MMDSOpts{LogsCollectorAddress: "http://collector.invalid"},
		client: http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			close(entered)
			<-release
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		})},
	}
	data := append([]byte(`{"data":"`), bytes.Repeat([]byte("x"), 64*1024)...)
	data = append(data, []byte(`"}`)...)
	w.Write(data)
	go func() { w.start(context.Background()); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("collector was not called")
	}
	for i := 0; i < 1024; i++ {
		w.Write(data)
	}
	w.logLock.Lock()
	queuedBytes, records := w.queuedLogBytes, len(w.logs)
	w.logLock.Unlock()
	// Discard the pending test batch, then let the one blocked request finish.
	w.getAllLogs()
	close(w.triggers)
	close(release)
	<-done
	if queuedBytes > maxQueuedLogBytes || records > maxQueuedLogRecords {
		t.Fatal("slow collector exceeded pending queue bound")
	}
	if queuedBytes < maxQueuedLogBytes-len(data) {
		t.Fatal("test did not fill queue")
	}
}
