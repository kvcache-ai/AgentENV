package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/contrib/snapshotservice"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) > 1 && os.Args[0] != "-" && os.Args[1] == "register" {
		runRegister()
		return
	}

	address := flag.String("address", "/run/containerd-plain-snapshotter/snapshotter.sock", "gRPC socket path")
	root := flag.String("root", "/var/lib/containerd-plain-snapshotter", "root directory for snapshotter state")
	flag.Parse()

	sn, err := NewSnapshotter(*root)
	if err != nil {
		log.Fatalf("failed to create snapshotter: %v", err)
	}

	rpc := grpc.NewServer()
	snapshotsapi.RegisterSnapshotsServer(rpc, snapshotservice.FromSnapshotter(sn))

	// Prepare socket
	if err := os.MkdirAll(filepath.Dir(*address), 0700); err != nil {
		log.Fatalf("failed to create socket directory: %v", err)
	}
	os.Remove(*address)

	l, err := net.Listen("unix", *address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		log.Println("shutting down...")
		rpc.GracefulStop()
	}()

	log.Printf("plain snapshotter listening on %s", *address)
	if err := rpc.Serve(l); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}

func runRegister() {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	root := fs.String("root", "/var/lib/containerd-plain-snapshotter", "root directory (must match server)")
	imageMetadata := fs.String("image-metadata", "", "image metadata JSON file containing ENV/ENTRYPOINT/CMD etc.")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s register [flags] <image-name> <rootfs-path>\n", os.Args[0])
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[2:])

	args := fs.Args()
	if len(args) != 2 {
		fs.Usage()
		os.Exit(1)
	}
	imageName := args[0]
	rootfsPath := args[1]

	// Validate rootfs
	absPath, err := filepath.Abs(rootfsPath)
	if err != nil {
		log.Fatalf("invalid rootfs path: %v", err)
	}
	info, err := os.Stat(absPath)
	if err != nil || !info.IsDir() {
		log.Fatalf("rootfs path %q is not a valid directory", absPath)
	}

	log.Printf("registering docker image %s -> %s ...", imageName, absPath)

	chainID, err := registerImage(*imageMetadata, imageName)
	if err != nil {
		log.Fatalf("register: %v", err)
	}

	log.Printf("chain ID: %s", chainID)

	// Save chain ID -> rootfs mapping
	sn, err := NewSnapshotter(*root)
	if err != nil {
		log.Fatalf("create snapshotter: %v", err)
	}
	if err := sn.RegisterRootfs(chainID, absPath); err != nil {
		log.Fatalf("register rootfs: %v", err)
	}

	log.Printf("registered: %s -> %s (chain ID: %s)", imageName, absPath, chainID)
	log.Printf("config saved to %s", filepath.Join(*root, "config.json"))
}

// registerImage builds a Docker image tar (old format) with a placeholder layer
// and loads it via `docker load`. If imageMetadata is provided, the image metadata
// is read from that file; otherwise an empty config is used.
func registerImage(imageMetadata, imageName string) (string, error) {
	// Read or create empty container config
	var parsed map[string]interface{}
	if imageMetadata != "" {
		data, err := os.ReadFile(imageMetadata)
		if err != nil {
			return "", fmt.Errorf("read image metadata file: %w", err)
		}
		if err := json.Unmarshal(data, &parsed); err != nil {
			return "", fmt.Errorf("parse config JSON: %w", err)
		}
	} else {
		parsed = map[string]interface{}{}
	}

	// Build the placeholder layer tar
	layerTar, err := buildUUIDTarBytes()
	if err != nil {
		return "", fmt.Errorf("build layer tar: %w", err)
	}

	// Compute diff ID (sha256 of uncompressed layer tar)
	diffIDHash := sha256.Sum256(layerTar)
	diffID := "sha256:" + hex.EncodeToString(diffIDHash[:])

	// Build the full image config JSON
	imageConfig := buildImageConfig(parsed, diffID)
	imageConfigJSON, err := json.Marshal(imageConfig)
	if err != nil {
		return "", fmt.Errorf("marshal image config: %w", err)
	}

	// Config hash is used as filename
	configHash := sha256.Sum256(imageConfigJSON)
	configHashStr := hex.EncodeToString(configHash[:])

	// Layer directory name (use diffID hex)
	layerDirName := hex.EncodeToString(diffIDHash[:])

	// Build manifest.json
	if strings.Index(imageName, ":") < 0 {
		imageName = imageName + ":latest"
	}
	manifest := []map[string]interface{}{
		{
			"Config":   configHashStr + ".json",
			"RepoTags": []string{imageName},
			"Layers":   []string{layerDirName + "/layer.tar"},
		},
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}

	// Build the docker load tar (old format):
	//   manifest.json
	//   <config-hash>.json
	//   <layer-hash>/layer.tar
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	if err := tarWriteFile(tw, "manifest.json", manifestJSON); err != nil {
		return "", err
	}
	if err := tarWriteFile(tw, configHashStr+".json", imageConfigJSON); err != nil {
		return "", err
	}
	if err := tarWriteFile(tw, layerDirName+"/layer.tar", layerTar); err != nil {
		return "", err
	}
	if err := tw.Close(); err != nil {
		return "", err
	}

	// docker load
	loadCmd := exec.Command("docker", "load")
	loadCmd.Stdin = &buf
	loadCmd.Stdout = os.Stderr
	loadCmd.Stderr = os.Stderr
	if err := loadCmd.Run(); err != nil {
		return "", fmt.Errorf("docker load: %w", err)
	}

	return inspectChainID(imageName)
}

// buildImageConfig wraps a user-provided config object into a full Docker
// image config with the correct rootfs.diff_ids.
// The input can be either a bare container config (.Config from docker inspect)
// or a full image config. We detect by checking for known top-level keys.
func buildImageConfig(parsed map[string]interface{}, diffID string) map[string]interface{} {
	// If it already looks like a full image config (has "rootfs" or "architecture"),
	// just override the rootfs section.
	if _, hasRootfs := parsed["rootfs"]; hasRootfs {
		parsed["rootfs"] = map[string]interface{}{
			"type":     "layers",
			"diff_ids": []string{diffID},
		}
		// Clear history since we're replacing layers
		parsed["history"] = []map[string]interface{}{
			{
				"created":     time.Now().UTC().Format(time.RFC3339),
				"comment":     "plain-snapshotter placeholder layer",
				"empty_layer": false,
			},
		}
		return parsed
	}

	// Otherwise treat it as a bare container config and wrap it.
	return map[string]interface{}{
		"architecture": "amd64",
		"os":           "linux",
		"config":       parsed,
		"rootfs": map[string]interface{}{
			"type":     "layers",
			"diff_ids": []string{diffID},
		},
		"history": []map[string]interface{}{
			{
				"created":     time.Now().UTC().Format(time.RFC3339),
				"comment":     "plain-snapshotter placeholder layer",
				"empty_layer": false,
			},
		},
	}
}

// inspectChainID extracts the chain ID of an image via docker inspect.
func inspectChainID(imageName string) (string, error) {
	out, err := exec.Command("docker", "image", "inspect", "--format", "{{json .RootFS}}", imageName).Output()
	if err != nil {
		return "", fmt.Errorf("docker inspect: %w", err)
	}

	var rootFS struct {
		Layers []string `json:"Layers"`
	}
	if err := json.Unmarshal(out, &rootFS); err != nil {
		return "", fmt.Errorf("parse RootFS: %w", err)
	}
	if len(rootFS.Layers) == 0 {
		return "", fmt.Errorf("image has no layers")
	}

	chainID := strings.TrimSpace(rootFS.Layers[len(rootFS.Layers)-1])
	return chainID, nil
}

// tarWriteFile adds a single file to a tar writer.
func tarWriteFile(tw *tar.Writer, name string, data []byte) error {
	if err := tw.WriteHeader(&tar.Header{
		Name:    name,
		Size:    int64(len(data)),
		Mode:    0644,
		ModTime: time.Now(),
	}); err != nil {
		return fmt.Errorf("tar write header %s: %w", name, err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("tar write %s: %w", name, err)
	}
	return nil
}

// buildUUIDTarBytes returns a complete tar archive as a byte slice.
func buildUUIDTarBytes() ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	content := []byte(uuid.NewString())
	if err := tw.WriteHeader(&tar.Header{
		Name:    ".plain-id",
		Size:    int64(len(content)),
		Mode:    0444,
		ModTime: time.Now(),
	}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(content); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
