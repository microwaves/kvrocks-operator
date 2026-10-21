package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

const testManifestKey = "backups/database-uid/backup-001/manifest.json"

type memoryStore struct {
	mu       sync.Mutex
	objects  map[string][]byte
	puts     []string
	gets     []string
	get      func(string) error
	read     func(string, []byte) io.ReadCloser
	put      func(string) error
	afterPut func(string) error
}

func newMemoryStore() *memoryStore {
	return &memoryStore{objects: make(map[string][]byte)}
}

func (s *memoryStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = append(s.gets, key)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.get != nil {
		if err := s.get(key); err != nil {
			return nil, err
		}
	}
	data, found := s.objects[key]
	if !found {
		return nil, ErrObjectNotFound
	}
	if s.read != nil {
		return s.read(key, bytes.Clone(data)), nil
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(data))), nil
}

func (s *memoryStore) PutIfAbsent(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts = append(s.puts, key)
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.put != nil {
		if err := s.put(key); err != nil {
			return err
		}
	}
	if _, exists := s.objects[key]; exists {
		return ErrObjectExists
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(data)) != size || manifestDigest(data) != digest {
		return errors.New("invalid upload body")
	}
	s.objects[key] = data
	if s.afterPut != nil {
		return s.afterPut(key)
	}
	return nil
}

func transferFixture(t *testing.T) (Transfer, *memoryStore, Artifact) {
	t.Helper()
	capturer, database := fixture(t)
	artifact, err := capturer.Capture(t.Context(), testRequest)
	if err != nil {
		t.Fatal(err)
	}
	if database.saves != 1 {
		t.Fatal("fixture must capture exactly once")
	}
	store := newMemoryStore()
	return Transfer{Store: store}, store, artifact
}

func uploadFixture(t *testing.T, transfer Transfer, artifact Artifact) {
	t.Helper()
	if err := transfer.Upload(t.Context(), artifact.Directory, testManifestKey, artifact.SHA256); err != nil {
		t.Fatal(err)
	}
}

func TestTransferRoundTrip(t *testing.T) {
	transfer, store, artifact := transferFixture(t)
	uploadFixture(t, transfer, artifact)
	if last := store.puts[len(store.puts)-1]; last != testManifestKey {
		t.Fatalf("manifest must be published last, got %q", last)
	}
	data, err := readDocument(filepath.Join(artifact.Directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(store.objects[testManifestKey], data) {
		t.Fatal("upload changed exact manifest bytes")
	}
	if err := os.RemoveAll(artifact.Directory); err != nil {
		t.Fatal(err)
	}
	// A lost successful response is recoverable even after the source disappears.
	uploadFixture(t, transfer, artifact)
	if len(store.puts) != len(artifact.Manifest.Files)+1 {
		t.Fatal("retry rewrote committed objects")
	}
	directory := filepath.Join(t.TempDir(), "download")
	downloaded, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, directory)
	if err != nil {
		t.Fatal(err)
	}
	if downloaded.SHA256 != artifact.SHA256 {
		t.Fatal("download changed manifest checksum")
	}
	if err := Materialize(t.Context(), directory, artifact.SHA256, filepath.Join(t.TempDir(), "db")); err != nil {
		t.Fatal(err)
	}
	store.get = func(string) error { return errors.New("offline") }
	if _, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, directory); err != nil {
		t.Fatalf("completed download should be reusable offline: %v", err)
	}
}

func TestUploadRetry(t *testing.T) {
	for _, point := range []string{"checkpoint", "manifest", "lost checkpoint response", "lost manifest response"} {
		t.Run(point, func(t *testing.T) {
			transfer, store, artifact := transferFixture(t)
			key := testManifestKey
			if strings.Contains(point, "checkpoint") {
				key = checkpointKey(testManifestKey, artifact.Manifest.Files[1].Name)
			}
			failure := func(candidate string) error {
				if candidate == key {
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			if strings.HasPrefix(point, "lost") {
				store.afterPut = failure
				uploadFixture(t, transfer, artifact)
			} else {
				store.put = failure
				if err := transfer.Upload(t.Context(), artifact.Directory, testManifestKey, artifact.SHA256); !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("expected interrupted upload, got %v", err)
				}
				if _, committed := store.objects[testManifestKey]; committed {
					t.Fatal("partial upload published a manifest")
				}
			}
			store.put, store.afterPut = nil, nil
			uploadFixture(t, transfer, artifact)
			firstKey := checkpointKey(testManifestKey, artifact.Manifest.Files[0].Name)
			if count := countKey(store.puts, firstKey); count != 1 {
				t.Fatalf("verified file uploaded %d times", count)
			}
		})
	}
}

func TestConcurrentUploads(t *testing.T) {
	transfer, store, artifact := transferFixture(t)
	errors := make(chan error, 2)
	for range 2 {
		go func() { errors <- transfer.Upload(t.Context(), artifact.Directory, testManifestKey, artifact.SHA256) }()
	}
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if len(store.objects) != len(artifact.Manifest.Files)+1 {
		t.Fatal("duplicate workers published different object sets")
	}
}

func TestUploadRefusesCorruptOrUnavailableObjects(t *testing.T) {
	for _, scenario := range []string{"file", "manifest", "missing committed file", "access denied"} {
		t.Run(scenario, func(t *testing.T) {
			transfer, store, artifact := transferFixture(t)
			fileKey := checkpointKey(testManifestKey, artifact.Manifest.Files[0].Name)
			switch scenario {
			case "file":
				store.objects[fileKey] = []byte("corrupt")
			case "manifest":
				store.objects[testManifestKey] = []byte("corrupt")
			case "missing committed file":
				uploadFixture(t, transfer, artifact)
				delete(store.objects, fileKey)
				store.puts = nil
			case "access denied":
				store.get = func(string) error { return errors.New("access denied") }
			}
			if err := transfer.Upload(t.Context(), artifact.Directory, testManifestKey, artifact.SHA256); err == nil {
				t.Fatal("accepted corrupt or inaccessible backup")
			}
			if len(store.puts) != 0 {
				t.Fatal("attempted to overwrite or repair committed objects")
			}
		})
	}
}

func countKey(keys []string, key string) int {
	count := 0
	for _, candidate := range keys {
		if candidate == key {
			count++
		}
	}
	return count
}

func TestTransferRejectsInvalidInputs(t *testing.T) {
	transfer, store, artifact := transferFixture(t)
	for _, key := range []string{"", "other.json", strings.Repeat("a", 1024) + "/manifest.json"} {
		if err := transfer.Upload(t.Context(), artifact.Directory, key, artifact.SHA256); err == nil {
			t.Fatalf("accepted invalid manifest key %q", key)
		}
	}
	if err := transfer.Upload(t.Context(), artifact.Directory, testManifestKey, "invalid"); err == nil {
		t.Fatal("accepted invalid digest")
	}
	if len(store.gets) != 0 || len(store.puts) != 0 {
		t.Fatal("invalid input reached object storage")
	}
	manifest := artifact.Manifest
	manifest.Files = slices.Clone(manifest.Files)
	manifest.Files[0].Size = maxObjectBytes + 1
	artifact.Manifest = manifest
	digest := replaceManifest(t, artifact)
	data, err := readDocument(filepath.Join(artifact.Directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	store.objects[testManifestKey] = data
	if _, err := transfer.Download(t.Context(), testManifestKey, digest, t.TempDir()); err == nil || !strings.Contains(err.Error(), "5 GiB") {
		t.Fatalf("expected explicit single-upload limit, got %v", err)
	}
	if len(store.gets) != 1 {
		t.Fatal("oversized manifest caused checkpoint downloads")
	}
}

func TestUploadRecoversUnknownManifestPublication(t *testing.T) {
	transfer, store, artifact := transferFixture(t)
	store.afterPut = func(key string) error {
		if key == testManifestKey {
			store.get = func(string) error { return io.ErrUnexpectedEOF }
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	if err := transfer.Upload(t.Context(), artifact.Directory, testManifestKey, artifact.SHA256); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected unknown publication outcome, got %v", err)
	}
	if err := os.RemoveAll(artifact.Directory); err != nil {
		t.Fatal(err)
	}
	store.get, store.afterPut = nil, nil
	uploadFixture(t, transfer, artifact)
	if countKey(store.puts, testManifestKey) != 1 {
		t.Fatal("retry rewrote the committed manifest")
	}
}
