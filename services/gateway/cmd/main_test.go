package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
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
