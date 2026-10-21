package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	gateway "agentenv/services/gateway/internal"
	"agentenv/services/shared/config"

	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const testAPIKey = "e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestValidateAPIKey(t *testing.T) {
	t.Parallel()

	got, err := validateAPIKey(testAPIKey, "test")
	if err != nil {
		t.Fatalf("validateAPIKey() error = %v", err)
	}
	if got != testAPIKey {
		t.Fatalf("validateAPIKey() = %q, want %q", got, testAPIKey)
	}

	for _, invalid := range []string{"", "too-short", " " + testAPIKey, testAPIKey + "\n", strings.Repeat("a", 31), strings.Repeat("a", maxAPIKeyLen+1), strings.Repeat("a", 31) + "!"} {
		if _, err := validateAPIKey(invalid, "test"); err == nil {
			t.Errorf("validateAPIKey(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestLoadAPIKeyFromEnvironment(t *testing.T) {
	got, err := loadAPIKeyFrom(
		func(name string) (string, bool) { return testAPIKey, name == apiKeyEnv },
		filepath.Join(t.TempDir(), "missing"),
	)
	if err != nil {
		t.Fatalf("loadAPIKey() error = %v", err)
	}
	if got != testAPIKey {
		t.Fatalf("loadAPIKey() = %q, want %q", got, testAPIKey)
	}
}

func TestLoadAPIKeyRejectsExplicitEmptyEnvironment(t *testing.T) {
	if _, err := loadAPIKeyFrom(
		func(name string) (string, bool) { return "", name == apiKeyEnv },
		filepath.Join(t.TempDir(), "missing"),
	); err == nil {
		t.Fatal("loadAPIKey() unexpectedly accepted an empty environment value")
	}
}

func TestLoadAPIKeyFromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "api-key")
	if err := os.WriteFile(path, []byte(testAPIKey+"\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	got, err := loadAPIKeyFrom(func(string) (string, bool) { return "", false }, path)
	if err != nil {
		t.Fatalf("loadAPIKeyFrom() error = %v", err)
	}
	if got != testAPIKey {
		t.Fatalf("loadAPIKeyFrom() = %q, want %q", got, testAPIKey)
	}
}

func TestLoadAPIKeyRejectsMissingFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missing := filepath.Join(dir, "missing")
	if _, err := loadAPIKeyFrom(func(string) (string, bool) { return "", false }, missing); err == nil {
		t.Fatal("loadAPIKeyFrom() unexpectedly accepted a missing secret")
	}
}

func TestLoadAPIKeyRejectsNonRegularFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "api-key")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAPIKeyFrom(func(string) (string, bool) { return "", false }, path); err == nil {
		t.Fatal("loadAPIKeyFrom() unexpectedly accepted a FIFO")
	}
}

func TestLoadAPIKeyAllowsSymlinkedSecret(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "..data-api-key")
	path := filepath.Join(dir, "api-key")
	if err := os.WriteFile(target, []byte(testAPIKey+"\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(target), path); err != nil {
		t.Fatal(err)
	}
	got, err := loadAPIKeyFrom(func(string) (string, bool) { return "", false }, path)
	if err != nil {
		t.Fatalf("loadAPIKeyFrom() error = %v", err)
	}
	if got != testAPIKey {
		t.Fatalf("loadAPIKeyFrom() = %q, want %q", got, testAPIKey)
	}
}

func localCertificate(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute),
		NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	for path, data := range map[string][]byte{
		certPath: certPEM,
		keyPath:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("cannot trust test certificate")
	}
	return certPath, keyPath, roots
}

type tlsTestScheduler struct {
	schedulerv1.SchedulerClient
	endpoint string
}

func (s tlsTestScheduler) LookupNode(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
	return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{Endpoint: s.endpoint}}, nil
}

func TestManualTLSGatewayToPlainHTTPNode(t *testing.T) {
	cert, key, roots := localCertificate(t)
	cfg := config.GatewayConfig{TLS: config.GatewayTLSConfig{Mode: "manual", CertFile: cert, KeyFile: key}}
	tlsConfig, start, cleanup, err := prepareTLS(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := start(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil {
			t.Error("node received TLS instead of HTTP")
		}
		if got := r.Header.Get("X-Forwarded-Proto"); got != "https" {
			t.Errorf("forwarded proto = %q", got)
		}
		if r.URL.RequestURI() != "/proxy/files/%2F?test=1" {
			t.Errorf("encoded path was changed: %s", r.URL.RequestURI())
		}
		if r.Header.Get("x-agentenv-sandbox-id") != "sandbox" || r.Header.Get("x-agentenv-target-port") != "8080" {
			t.Error("host-based sandbox routing was lost")
		}
		w.Write([]byte("node-response"))
	}))
	defer backend.Close()
	gw, err := gateway.NewServer(zap.NewNop(), tlsTestScheduler{endpoint: backend.URL}, gateway.ServerOptions{
		APIKey: testAPIKey, RequestTimeout: time.Second, SandboxProxyDomains: []string{"sandbox.example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewUnstartedServer(gw.Handler())
	front.TLS = tlsConfig
	front.StartTLS()
	defer front.Close()
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	req, _ := http.NewRequest("GET", front.URL+"/files/%2F?test=1", nil)
	req.Host = "8080-sandbox.sandbox.example.com"
	req.Header.Set("X-API-Key", testAPIKey)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "node-response" {
		t.Fatalf("gateway response: %d %s", resp.StatusCode, body)
	}
	req.Header.Del("X-API-Key")
	// Use the API host to exercise gateway API-key authentication.
	req.Host = "api.example.com"
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated request returned %d", resp.StatusCode)
	}
	// HTTPS-only endpoints must reject plaintext; no silent HTTP fallback.
	resp, err = http.Get("http://" + front.Listener.Addr().String() + "/health")
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode < 400 {
			t.Fatalf("plaintext request unexpectedly succeeded: %d", resp.StatusCode)
		}
	}
}

func TestPrepareTLSModes(t *testing.T) {
	tlsConfig, _, closeTLS, err := prepareTLS(config.GatewayConfig{}, zap.NewNop())
	if err != nil || tlsConfig != nil {
		t.Fatalf("default should remain HTTP: %v", err)
	}
	closeTLS()
	_, _, _, err = prepareTLS(config.GatewayConfig{TLS: config.GatewayTLSConfig{
		Mode: "manual", CertFile: "missing-cert", KeyFile: "missing-key",
	}}, zap.NewNop())
	if err == nil {
		t.Fatal("missing certificates accepted")
	}
	_, _, _, err = prepareTLS(config.GatewayConfig{
		HTTPSListenAddr: ":8443",
		TLS:             config.GatewayTLSConfig{Mode: "acme", Challenge: "dns-01"},
	}, zap.NewNop())
	if err == nil {
		t.Fatal("missing DNS credential accepted")
	}
	// Construction alone must not contact a CA; issuance starts explicitly.
	tlsConfig, _, closeTLS, err = prepareTLS(config.GatewayConfig{
		HTTPSListenAddr: ":8443",
		TLS:             config.GatewayTLSConfig{Mode: "acme", StorageDir: t.TempDir()},
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer closeTLS()
	found := false
	for _, protocol := range tlsConfig.NextProtos {
		found = found || protocol == "acme-tls/1"
	}
	if !found || tlsConfig.GetCertificate == nil || tlsConfig.MinVersion < tls.VersionTLS12 {
		t.Fatal("ACME listener lacks TLS-ALPN or certificate callback")
	}
}

func TestTLSGatewayStreamingAndWebSocket(t *testing.T) {
	cert, key, roots := localCertificate(t)
	tlsConfig, _, cleanup, err := prepareTLS(config.GatewayConfig{
		TLS: config.GatewayTLSConfig{Mode: "manual", CertFile: cert, KeyFile: key},
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxy/events":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			// The client must receive the first event before this response ends.
			<-r.Context().Done()
		case "/proxy/ws":
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			hash := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n",
				base64.StdEncoding.EncodeToString(hash[:]))
			rw.Flush()
			frame := make([]byte, 8)
			if _, err := io.ReadFull(rw, frame); err != nil {
				t.Error(err)
				return
			}
			if frame[0] != 0x81 || frame[1] != 0x82 || frame[6]^frame[2] != 'h' || frame[7]^frame[3] != 'i' {
				t.Error("WebSocket frame changed in transit")
				return
			}
			rw.Write([]byte{0x81, 2, 'o', 'k'})
			rw.Flush()
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	gw, err := gateway.NewServer(zap.NewNop(), tlsTestScheduler{endpoint: backend.URL}, gateway.ServerOptions{
		APIKey: testAPIKey, RequestTimeout: time.Second, SandboxProxyDomains: []string{"sandbox.example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewUnstartedServer(gw.Handler())
	front.TLS = tlsConfig
	front.StartTLS()
	defer front.Close()
	t.Run("SSE", func(t *testing.T) {
		transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
		req, _ := http.NewRequest("GET", front.URL+"/events", nil)
		req.Host = "8080-sandbox.sandbox.example.com"
		req.Header.Set("Accept", "text/event-stream")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		line, err := bufio.NewReader(resp.Body).ReadString('\n')
		if err != nil || line != "data: first\n" {
			t.Fatalf("event did not arrive before response completion: %q, %v", line, err)
		}
	})
	t.Run("WSS", func(t *testing.T) {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp",
			front.Listener.Addr().String(), &tls.Config{RootCAs: roots})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		fmt.Fprint(conn, "GET /ws HTTP/1.1\r\nHost: 8080-sandbox.sandbox.example.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
		reader := bufio.NewReader(conn)
		status, err := reader.ReadString('\n')
		if err != nil || !strings.Contains(status, "101") {
			t.Fatalf("WebSocket upgrade failed: %q %v", status, err)
		}
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if line == "\r\n" {
				break
			}
		}
		conn.Write([]byte{0x81, 0x82, 1, 2, 3, 4, 'h' ^ 1, 'i' ^ 2})
		frame := make([]byte, 4)
		if _, err := io.ReadFull(reader, frame); err != nil {
			t.Fatal(err)
		}
		if string(frame) != "\x81\x02ok" {
			t.Fatalf("bad WebSocket reply: %x", frame)
		}
	})
}

func TestIPCertificateProfile(t *testing.T) {
	for _, tc := range []struct {
		ca    string
		names []string
		want  string
	}{
		{"", []string{"8.8.8.8"}, "shortlived"},
		{certmagic.LetsEncryptProductionCA, []string{"2606:4700:4700::1111"}, "shortlived"},
		{certmagic.LetsEncryptStagingCA, []string{"api.example.com", "8.8.8.8"}, "shortlived"},
		{"", []string{"api.example.com"}, ""},
		{"https://ca.example.com/directory", []string{"8.8.8.8"}, ""},
	} {
		if got := certificateProfile(tc.ca, tc.names); got != tc.want {
			t.Errorf("profile(%q, %v) = %q, want %q", tc.ca, tc.names, got, tc.want)
		}
	}
}

func TestACMEIPLookupWithoutSNI(t *testing.T) {
	cfg := config.GatewayConfig{HTTPSListenAddr: ":8443", TLS: config.GatewayTLSConfig{
		Mode: "acme", Domains: []string{"8.8.8.8"}, StorageDir: t.TempDir(),
	}}
	tlsConfig, _, cleanup, err := prepareTLS(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	// No issuance is started. The missing-certificate error must name the public
	// identifier, not the socket's private/local address (or an empty SNI).
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	_, err = tlsConfig.GetCertificate(&tls.ClientHelloInfo{Conn: conn})
	if err == nil || !strings.Contains(err.Error(), "8.8.8.8") {
		t.Fatalf("unexpected no-SNI certificate lookup: %v", err)
	}
}

func TestPrepareDNSUsesConfiguredToken(t *testing.T) {
	// Prepare only: this must not contact Cloudflare or a CA.
	_, _, cleanup, err := prepareTLS(config.GatewayConfig{
		HTTPSListenAddr: ":8080",
		TLS:             config.GatewayTLSConfig{Mode: "acme", Challenge: "dns-01", DNSProvider: "cloudflare", DNSConfig: []byte(`{"api_token":"test-config-token"}`), StorageDir: t.TempDir()},
	}, zap.NewNop())
	if err != nil {
		t.Fatal("failed to prepare DNS-01 using configured credential")
	}
	defer cleanup()
}

func TestDNSProviders(t *testing.T) {
	for _, tc := range []struct{ name, raw, kind string }{
		{"cloudflare", `{"api_token":"test-token","zone_token":"test-zone"}`, "*cloudflare.Provider"},
		{"alidns", `{"access_key_id":"test-id","access_key_secret":"test-secret","security_token":"test-session"}`, "*alidns.Provider"},
		{"tencentcloud", `{"SecretId":"test-id","SecretKey":"test-secret","SessionToken":"test-session"}`, "*tencentcloud.Provider"},
		{"route53", `{"access_key_id":"test-id","secret_access_key":"test-secret","region":"us-east-1","max_retries":3}`, "*route53.Provider"},
		{"route53", `{}`, "*route53.Provider"},
	} {
		t.Run(tc.name+tc.raw, func(t *testing.T) {
			provider, err := newDNSProvider(tc.name, []byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprintf("%T", provider) != tc.kind {
				t.Fatal("wrong provider type")
			}
			// Verify provider-native fields survive decoding, including non-string options.
			encoded, err := json.Marshal(provider)
			if err != nil {
				t.Fatal(err)
			}
			var want, got map[string]any
			json.Unmarshal([]byte(tc.raw), &want)
			json.Unmarshal(encoded, &got)
			for key, value := range want {
				if got[key] != value {
					t.Fatalf("provider field %s not forwarded", key)
				}
			}
			_, _, cleanup, err := prepareTLS(config.GatewayConfig{HTTPSListenAddr: ":8080", TLS: config.GatewayTLSConfig{Mode: "acme", Challenge: "dns-01", DNSProvider: tc.name, DNSConfig: []byte(tc.raw), StorageDir: t.TempDir()}}, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			cleanup()
		})
	}
}

func TestDNSProviderRejectsInvalidConfig(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"unknown", `{}`}, {"cloudflare", `null`}, {"cloudflare", `[]`},
		{"cloudflare", `{}`}, {"cloudflare", `{"api_token":123}`},
		{"cloudflare", `{"api_token":"test","sensitive-unknown-key":"secret"}`},
		{"cloudflare", `{"api_token":"test"} {}`},
		{"alidns", `{"access_key_id":"test"}`},
		{"tencentcloud", `{"secret_id":"test","secret_key":"secret"}`},
		{"route53", `{"access_key_id":"test"}`},
	} {
		_, err := newDNSProvider(tc.name, []byte(tc.raw))
		if err == nil {
			t.Fatalf("accepted invalid config for %s", tc.name)
		}
		if strings.Contains(err.Error(), "sensitive-unknown-key") || strings.Contains(err.Error(), "secret") {
			t.Fatal("error exposed credentials")
		}
	}
}
