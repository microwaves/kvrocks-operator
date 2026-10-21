package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

const maxObjectBytes int64 = 5 << 30

var (
	ErrObjectNotFound = errors.New("object does not exist")
	ErrObjectExists   = errors.New("object already exists")
)

// PutIfAbsent must create atomically and never replace an existing object.
// Its body is seekable so a transport retry can resend the same bytes.
type ObjectStore interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	PutIfAbsent(ctx context.Context, key string, body io.ReadSeeker, size int64, sha256 string) error
}

type Transfer struct {
	Store ObjectStore
}

// Upload publishes checkpoint files before the manifest. Retrying the same key
// and digest observes existing objects, including a PUT whose response was lost.
func (t Transfer) Upload(ctx context.Context, directory, manifestKey, digest string) error {
	if err := t.validate(manifestKey, digest); err != nil {
		return err
	}
	data, err := t.readManifest(ctx, manifestKey)
	if err == nil {
		return t.verifyRemoteArtifact(ctx, manifestKey, data, digest)
	}
	if !errors.Is(err, ErrObjectNotFound) {
		return err
	}

	data, err = readDocument(filepath.Join(directory, "manifest.json"))
	if err != nil {
		return err
	}
	manifest, err := transferManifest(data, manifestKey, digest)
	if err != nil {
		return err
	}
	if err := verifyCheckpointFiles(ctx, filepath.Join(directory, "checkpoint"), manifest.Files); err != nil {
		return err
	}
	for _, file := range manifest.Files {
		key := checkpointKey(manifestKey, file.Name)
		if err := t.uploadFile(ctx, filepath.Join(directory, "checkpoint", file.Name), key, file); err != nil {
			return err
		}
	}

	metadata := File{Name: "manifest.json", Size: int64(len(data)), SHA256: digest}
	return t.publishObject(ctx, manifestKey, bytes.NewReader(data), metadata)
}

func (t Transfer) validate(manifestKey, digest string) error {
	if t.Store == nil {
		return fmt.Errorf("object transfer requires an object store")
	}
	if !validDigest(digest) {
		return fmt.Errorf("expected manifest SHA-256 must be 64 lowercase hexadecimal characters")
	}
	if manifestKey != "manifest.json" && !strings.HasSuffix(manifestKey, "/manifest.json") {
		return fmt.Errorf("manifest key %q must end in /manifest.json", manifestKey)
	}
	if len(manifestKey) > 1024 {
		return fmt.Errorf("manifest key exceeds S3's 1024-byte limit")
	}
	return nil
}

func checkpointKey(manifestKey, name string) string {
	// S3 keys are opaque: path.Join would collapse meaningful slashes or dots.
	return strings.TrimSuffix(manifestKey, "manifest.json") + "checkpoint/" + name
}

func validateTransferFiles(manifestKey string, files []File) error {
	for _, file := range files {
		if file.Size > maxObjectBytes {
			return fmt.Errorf("checkpoint file %q exceeds the 5 GiB single-upload limit", file.Name)
		}
		if len(checkpointKey(manifestKey, file.Name)) > 1024 {
			return fmt.Errorf("object key for checkpoint file %q exceeds S3's 1024-byte limit", file.Name)
		}
	}
	return nil
}

func (t Transfer) uploadFile(ctx context.Context, name, key string, expected File) (err error) {
	if err := t.verifyObject(ctx, key, expected, io.Discard); !errors.Is(err, ErrObjectNotFound) {
		return err
	}
	file, err := openRegular(name)
	if err != nil {
		return fmt.Errorf("open upload source %s: %w", name, err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	return t.publishObject(ctx, key, file, expected)
}

func (t Transfer) publishObject(ctx context.Context, key string, body io.ReadSeeker, expected File) error {
	putErr := t.Store.PutIfAbsent(ctx, key, body, expected.Size, expected.SHA256)
	// A failed response does not tell us whether the server stored the object.
	if err := t.verifyObject(ctx, key, expected, io.Discard); err != nil {
		return fmt.Errorf("publish object %q: %w", key, errors.Join(putErr, err))
	}
	return nil
}

func (t Transfer) readManifest(ctx context.Context, key string) (data []byte, err error) {
	body, err := t.Store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read manifest object %q: %w", key, err)
	}
	defer func() { err = errors.Join(err, body.Close()) }()
	data, err = io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: body}, maxManifestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read manifest object %q: %w", key, err)
	}
	if len(data) > maxManifestBytes {
		return nil, fmt.Errorf("manifest object %q exceeds the %d-byte size limit", key, maxManifestBytes)
	}
	return data, nil
}

func transferManifest(data []byte, manifestKey, digest string) (Manifest, error) {
	if actual := manifestDigest(data); actual != digest {
		return Manifest{}, fmt.Errorf("manifest object %q checksum mismatch: got %s, expected %s", manifestKey, actual, digest)
	}
	var manifest Manifest
	if err := decodeJSON(data, &manifest); err != nil {
		return Manifest{}, err
	}
	if err := manifest.validate(); err != nil {
		return Manifest{}, err
	}
	if err := validateTransferFiles(manifestKey, manifest.Files); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (t Transfer) verifyRemoteArtifact(ctx context.Context, key string, data []byte, digest string) error {
	manifest, err := transferManifest(data, key, digest)
	if err != nil {
		return err
	}
	for _, file := range manifest.Files {
		if err := t.verifyObject(ctx, checkpointKey(key, file.Name), file, io.Discard); err != nil {
			return err
		}
	}
	return nil
}

func (t Transfer) verifyObject(ctx context.Context, key string, expected File, destination io.Writer) (err error) {
	body, err := t.Store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("read object %q: %w", key, err)
	}
	defer func() { err = errors.Join(err, body.Close()) }()
	hash := sha256.New()
	reader := io.LimitReader(contextReader{ctx: ctx, reader: body}, expected.Size+1)
	size, err := io.Copy(io.MultiWriter(destination, hash), reader)
	if err != nil {
		return fmt.Errorf("read object %q: %w", key, err)
	}
	if size != expected.Size || fmt.Sprintf("%x", hash.Sum(nil)) != expected.SHA256 {
		return fmt.Errorf("object %q failed size/SHA-256 verification", key)
	}
	return nil
}
