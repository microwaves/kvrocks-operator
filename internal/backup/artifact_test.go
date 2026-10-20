package backup

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactRejectsDamage(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, Artifact) string
	}{
		{"wrong manifest digest", func(t *testing.T, a Artifact) string { return strings.Repeat("0", 64) }},
		{"changed file", func(t *testing.T, a Artifact) string {
			writeTestFile(t, filepath.Join(a.Directory, "checkpoint", "000001.log"), []byte("tampered"))
			return a.SHA256
		}},
		{"extra file", func(t *testing.T, a Artifact) string {
			writeTestFile(t, filepath.Join(a.Directory, "checkpoint", "extra"), []byte("unexpected"))
			return a.SHA256
		}},
		{"symlink", func(t *testing.T, a Artifact) string {
			name := filepath.Join(a.Directory, "checkpoint", "000001.log")
			if err := os.Remove(name); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("/etc/passwd", name); err != nil {
				t.Fatal(err)
			}
			return a.SHA256
		}},
		{"path traversal", func(t *testing.T, a Artifact) string {
			a.Manifest.Files[0].Name = "../../escape"
			return replaceManifest(t, a)
		}},
		{"duplicate file", func(t *testing.T, a Artifact) string {
			a.Manifest.Files = append(a.Manifest.Files, a.Manifest.Files[0])
			return replaceManifest(t, a)
		}},
		{"unsupported version", func(t *testing.T, a Artifact) string {
			a.Manifest.KvrocksVersion = "2.16.0"
			return replaceManifest(t, a)
		}},
		{"unknown format", func(t *testing.T, a Artifact) string {
			a.Manifest.Format = "something-else"
			return replaceManifest(t, a)
		}},
		{"unknown field", func(t *testing.T, a Artifact) string {
			data, err := json.Marshal(a.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			data = append([]byte(`{"unknown":true,`), data[1:]...)
			writeTestFile(t, filepath.Join(a.Directory, "manifest.json"), data)
			return fmt.Sprintf("%x", sha256.Sum256(data))
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			capturer, _ := fixture(t)
			artifact, err := capturer.Capture(t.Context(), testRequest)
			if err != nil {
				t.Fatal(err)
			}
			digest := test.mutate(t, artifact)
			if _, err := Verify(t.Context(), artifact.Directory, digest); err == nil {
				t.Fatal("accepted invalid artifact")
			}
		})
	}
}

func TestMaterializeRequiresFreshDestination(t *testing.T) {
	capturer, _ := fixture(t)
	artifact, err := capturer.Capture(t.Context(), testRequest)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "db")
	if err := Materialize(t.Context(), artifact.Directory, artifact.SHA256, target); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(target, "000001.log"))
	if err != nil || string(contents) != "unflushed data" {
		t.Fatalf("WAL not copied: %q, %v", contents, err)
	}
	if err := Materialize(t.Context(), artifact.Directory, artifact.SHA256, target); err == nil {
		t.Fatal("overwrote an existing database directory")
	}
}

func TestPublishDoesNotReplaceManifest(t *testing.T) {
	name := filepath.Join(t.TempDir(), "manifest.json")
	if err := publishJSON(name, "original"); err != nil {
		t.Fatal(err)
	}
	if err := publishJSON(name, "replacement"); err == nil {
		t.Fatal("replaced committed manifest")
	}
	var result string
	if err := readJSON(name, &result); err != nil || result != "original" {
		t.Fatalf("lost original: %q, %v", result, err)
	}
}

func replaceManifest(t *testing.T, artifact Artifact) string {
	t.Helper()
	data, err := json.Marshal(artifact.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(artifact.Directory, "manifest.json"), data)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func writeTestFile(t *testing.T, name string, contents []byte) {
	t.Helper()
	if err := os.WriteFile(name, contents, 0600); err != nil {
		t.Fatal(err)
	}
}
