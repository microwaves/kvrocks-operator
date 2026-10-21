package backup

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadRetry(t *testing.T) {
	transfer, store, artifact := transferFixture(t)
	uploadFixture(t, transfer, artifact)
	store.gets = nil
	failedKey := checkpointKey(testManifestKey, artifact.Manifest.Files[1].Name)
	store.get = func(key string) error {
		if key == failedKey {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	directory := t.TempDir()
	if _, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, directory); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected interrupted download, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete download published manifest: %v", err)
	}
	// Simulate a worker crash leaving an unfinished temporary file.
	writeTestFile(t, filepath.Join(directory, "download.tmp"), []byte("partial"))
	store.get = nil
	if _, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, directory); err != nil {
		t.Fatal(err)
	}
	firstKey := checkpointKey(testManifestKey, artifact.Manifest.Files[0].Name)
	if count := countKey(store.gets, firstKey); count != 1 {
		t.Fatalf("verified file downloaded %d times", count)
	}
}

func TestDownloadRefusesUnsafeReuse(t *testing.T) {
	for _, scenario := range []string{"different digest", "different key", "corrupt file", "corrupt manifest", "unowned directory", "directory symlink", "checkpoint symlink"} {
		t.Run(scenario, func(t *testing.T) {
			transfer, store, artifact := transferFixture(t)
			uploadFixture(t, transfer, artifact)
			directory := t.TempDir()
			key, digest := testManifestKey, artifact.SHA256
			switch scenario {
			case "unowned directory":
				writeTestFile(t, filepath.Join(directory, "database"), []byte("existing data"))
			case "directory symlink":
				target := directory
				directory = filepath.Join(t.TempDir(), "link")
				if err := os.Symlink(target, directory); err != nil {
					t.Fatal(err)
				}
			case "checkpoint symlink":
				if err := reserveDownload(directory, downloadIntent{ManifestKey: key, SHA256: digest}); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), filepath.Join(directory, "checkpoint")); err != nil {
					t.Fatal(err)
				}
			default:
				if _, err := transfer.Download(t.Context(), key, digest, directory); err != nil {
					t.Fatal(err)
				}
				switch scenario {
				case "different digest":
					digest = strings.Repeat("0", 64)
				case "different key":
					key = "another/manifest.json"
				case "corrupt file":
					writeTestFile(t, filepath.Join(directory, "checkpoint", artifact.Manifest.Files[0].Name), []byte("corrupt"))
				case "corrupt manifest":
					writeTestFile(t, filepath.Join(directory, "manifest.json"), []byte("corrupt"))
				}
			}
			store.gets = nil
			if _, err := transfer.Download(t.Context(), key, digest, directory); err == nil {
				t.Fatal("accepted unsafe download target")
			}
			if scenario != "checkpoint symlink" && len(store.gets) != 0 {
				t.Fatal("attempted to repair or replace existing local data")
			}
		})
	}
}

func TestDownloadRejectsDamagedObjects(t *testing.T) {
	for _, scenario := range []string{"missing", "truncated", "larger", "same size corruption", "wrong manifest digest", "oversized manifest"} {
		t.Run(scenario, func(t *testing.T) {
			transfer, store, artifact := transferFixture(t)
			uploadFixture(t, transfer, artifact)
			key := checkpointKey(testManifestKey, artifact.Manifest.Files[0].Name)
			switch scenario {
			case "missing":
				delete(store.objects, key)
			case "truncated":
				store.objects[key] = store.objects[key][:1]
			case "larger":
				store.objects[key] = append(store.objects[key], 'x')
			case "same size corruption":
				store.objects[key][0] ^= 1
			case "wrong manifest digest":
				store.objects[testManifestKey] = []byte("{}")
			case "oversized manifest":
				store.objects[testManifestKey] = make([]byte, maxManifestBytes+1)
			}
			directory := t.TempDir()
			if _, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, directory); err == nil {
				t.Fatal("accepted damaged object")
			}
			if _, err := os.Stat(filepath.Join(directory, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("damaged download published manifest: %v", err)
			}
		})
	}
}

func TestTransferCancellation(t *testing.T) {
	transfer, store, artifact := transferFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := transfer.Upload(ctx, artifact.Directory, testManifestKey, artifact.SHA256); !errors.Is(err, context.Canceled) {
		t.Fatalf("upload ignored cancellation: %v", err)
	}
	if _, err := transfer.Download(ctx, testManifestKey, artifact.SHA256, t.TempDir()); !errors.Is(err, context.Canceled) {
		t.Fatalf("download ignored cancellation: %v", err)
	}
	if len(store.puts) != 0 {
		t.Fatal("canceled upload attempted publication")
	}
}

type interruptedDownload struct {
	reader io.Reader
	closed bool
}

func (r *interruptedDownload) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func (r *interruptedDownload) Close() error {
	r.closed = true
	return nil
}

func TestDownloadRetriesInterruptedBody(t *testing.T) {
	transfer, store, artifact := transferFixture(t)
	uploadFixture(t, transfer, artifact)
	key := checkpointKey(testManifestKey, artifact.Manifest.Files[0].Name)
	interrupted := &interruptedDownload{reader: strings.NewReader("part")}
	store.read = func(candidate string, data []byte) io.ReadCloser {
		if candidate == key {
			return interrupted
		}
		return io.NopCloser(strings.NewReader(string(data)))
	}
	directory := t.TempDir()
	if _, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, directory); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected interrupted response body, got %v", err)
	}
	if !interrupted.closed {
		t.Fatal("interrupted response body was not closed")
	}
	if _, err := os.Stat(filepath.Join(directory, "checkpoint", artifact.Manifest.Files[0].Name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published a partial file: %v", err)
	}
	store.read = nil
	if _, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, directory); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentDownloads(t *testing.T) {
	transfer, store, artifact := transferFixture(t)
	uploadFixture(t, transfer, artifact)
	store.gets = nil
	directory := t.TempDir()
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, directory)
			results <- err
		}()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if len(store.gets) != len(artifact.Manifest.Files)+1 {
		t.Fatal("duplicate workers downloaded an artifact more than once")
	}
}
