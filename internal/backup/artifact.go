package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	artifactFormat   = "kvrocks-operator.org/checkpoint/v1alpha1"
	maxManifestBytes = 16 << 20
)

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)

type Source struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
	VolumeUID string `json:"volumeUID"`
}

type Request struct {
	ID     string `json:"id"`
	Source Source `json:"source"`
}

type File struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Format          string  `json:"format"`
	Request         Request `json:"request"`
	KvrocksVersion  string  `json:"kvrocksVersion"`
	Encoding        string  `json:"encoding"`
	ServerProcessID int64   `json:"serverProcessID"`
	// These bound capture; neither is an exact RocksDB sequence timestamp.
	StartedAt  time.Time `json:"startedAt"`
	CapturedAt time.Time `json:"capturedAt"`
	Files      []File    `json:"files"`
}

type Artifact struct {
	Directory string
	SHA256    string
	Manifest  Manifest
}

func (r Request) validate() error {
	fields := []struct {
		name  string
		value string
	}{
		{"id", r.ID},
		{"source.namespace", r.Source.Namespace},
		{"source.name", r.Source.Name},
		{"source.uid", r.Source.UID},
		{"source.volumeUID", r.Source.VolumeUID},
	}
	for _, field := range fields {
		if !identifier.MatchString(field.value) {
			return fmt.Errorf("invalid %s %q: require 1-128 letters, digits, hyphens or underscores, starting with a letter or digit", field.name, field.value)
		}
	}
	return nil
}

func (m Manifest) validate() error {
	if m.Format != artifactFormat {
		return fmt.Errorf("unsupported artifact format %q: expected %q", m.Format, artifactFormat)
	}
	if m.KvrocksVersion != supportedKvrocksVersion {
		return fmt.Errorf("unsupported artifact Kvrocks version %q: expected %s", m.KvrocksVersion, supportedKvrocksVersion)
	}
	if m.Encoding != "standalone" {
		return fmt.Errorf("unsupported artifact encoding %q: expected standalone", m.Encoding)
	}
	if err := m.Request.validate(); err != nil {
		return err
	}
	if m.ServerProcessID <= 0 {
		return fmt.Errorf("artifact has invalid server process ID %d", m.ServerProcessID)
	}
	if m.StartedAt.IsZero() {
		return fmt.Errorf("artifact is missing its capture start time")
	}
	if m.CapturedAt.Before(m.StartedAt) {
		return fmt.Errorf("artifact capture time %s precedes start time %s", m.CapturedAt, m.StartedAt)
	}
	if len(m.Files) == 0 {
		return fmt.Errorf("artifact has no checkpoint files")
	}
	previous := ""
	for _, file := range m.Files {
		if err := file.validate(); err != nil {
			return err
		}
		if file.Name <= previous {
			return fmt.Errorf("checkpoint files must be unique and sorted: %q follows %q", file.Name, previous)
		}
		previous = file.Name
	}
	return nil
}

func (f File) validate() error {
	if !safeName(f.Name) {
		return fmt.Errorf("checkpoint file has unsafe name %q", f.Name)
	}
	if f.Size < 0 {
		return fmt.Errorf("checkpoint file %q has negative size %d", f.Name, f.Size)
	}
	if !validDigest(f.SHA256) {
		return fmt.Errorf("checkpoint file %q has invalid SHA-256 %q", f.Name, f.SHA256)
	}
	return nil
}

func safeName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00")
}

func validDigest(digest string) bool {
	bytes, err := hex.DecodeString(digest)
	return err == nil && len(bytes) == sha256.Size && digest == strings.ToLower(digest)
}

func hashFile(ctx context.Context, file *os.File, destination io.Writer) (File, error) {
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(hash, destination), contextReader{ctx: ctx, reader: file})
	if err != nil {
		return File{}, fmt.Errorf("read or copy checkpoint file %s: %w", file.Name(), err)
	}
	return File{
		Name:   filepath.Base(file.Name()),
		Size:   size,
		SHA256: hex.EncodeToString(hash.Sum(nil)),
	}, nil
}

func syncAndHashFile(ctx context.Context, name string) (result File, err error) {
	file, err := openRegular(name)
	if err != nil {
		return File{}, fmt.Errorf("open checkpoint file %s: %w", name, err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	result, err = hashFile(ctx, file, io.Discard)
	if err != nil {
		return File{}, err
	}
	if err := file.Sync(); err != nil {
		return File{}, fmt.Errorf("sync checkpoint file %s: %w", name, err)
	}
	return result, nil
}

func checkpointFiles(directory string) ([]string, error) {
	exists, err := directoryExists(directory)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("checkpoint directory %s is missing", directory)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !safeName(entry.Name()) || !entry.Type().IsRegular() {
			return nil, fmt.Errorf("checkpoint entry %q is not a flat regular file", entry.Name())
		}
		names = append(names, entry.Name())
	}
	if err := validateCurrentManifest(directory, names); err != nil {
		return nil, err
	}
	return names, nil
}

// RocksDB's CURRENT must name a MANIFEST included in the same checkpoint.
func validateCurrentManifest(directory string, names []string) error {
	file, err := openRegular(filepath.Join(directory, "CURRENT"))
	if err != nil {
		return fmt.Errorf("read checkpoint CURRENT: %w", err)
	}
	current, readErr := io.ReadAll(io.LimitReader(file, 256))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return err
	}
	manifest := strings.TrimSuffix(string(current), "\n")
	if !strings.HasPrefix(manifest, "MANIFEST-") || !safeName(manifest) || !slices.Contains(names, manifest) {
		return fmt.Errorf("CURRENT references missing or invalid manifest %q", manifest)
	}
	return nil
}

// Verify checks a local artifact against an independently supplied manifest digest.
func Verify(ctx context.Context, directory, expectedSHA256 string) (Artifact, error) {
	if !validDigest(expectedSHA256) {
		return Artifact{}, fmt.Errorf("expected manifest SHA-256 must be 64 lowercase hexadecimal characters")
	}
	name := filepath.Join(directory, "manifest.json")
	data, err := readDocument(name)
	if err != nil {
		return Artifact{}, err
	}
	return verifyArtifact(ctx, directory, data, expectedSHA256)
}

func manifestDigest(data []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func verifyArtifact(ctx context.Context, directory string, data []byte, expectedSHA256 string) (Artifact, error) {
	digest := manifestDigest(data)
	if digest != expectedSHA256 {
		return Artifact{}, fmt.Errorf("manifest checksum mismatch: got %s, expected %s", digest, expectedSHA256)
	}
	var manifest Manifest
	if err := decodeJSON(data, &manifest); err != nil {
		return Artifact{}, err
	}
	if err := manifest.validate(); err != nil {
		return Artifact{}, err
	}
	if err := verifyCheckpointFiles(ctx, filepath.Join(directory, "checkpoint"), manifest.Files); err != nil {
		return Artifact{}, err
	}
	return Artifact{Directory: directory, SHA256: expectedSHA256, Manifest: manifest}, nil
}

func verifyCheckpointFiles(ctx context.Context, directory string, files []File) error {
	names, err := checkpointFiles(directory)
	if err != nil {
		return err
	}
	if len(names) != len(files) {
		return fmt.Errorf("checkpoint contains %d files; manifest requires %d", len(names), len(files))
	}
	for i, expected := range files {
		if names[i] != expected.Name {
			return fmt.Errorf("unexpected checkpoint file %q; expected %q", names[i], expected.Name)
		}
		if err := verifyCheckpointFile(ctx, filepath.Join(directory, expected.Name), expected); err != nil {
			return err
		}
	}
	return nil
}

func verifyCheckpointFile(ctx context.Context, name string, expected File) (err error) {
	file, err := openRegular(name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	actual, err := hashFile(ctx, file, io.Discard)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("checkpoint file %q failed size/SHA-256 verification", expected.Name)
	}
	return nil
}

// Materialize copies a verified checkpoint into an exclusively created directory.
// The caller must keep the target offline; database lifecycle is not managed here.
func Materialize(ctx context.Context, directory, digest, target string) error {
	artifact, err := Verify(ctx, directory, digest)
	if err != nil {
		return err
	}
	if err := os.Mkdir(target, 0700); err != nil {
		return fmt.Errorf("create fresh restore directory %s: %w", target, err)
	}
	// An interrupted copy deliberately remains incomplete and cannot be overwritten.
	checkpoint := filepath.Join(directory, "checkpoint")
	for _, expected := range artifact.Manifest.Files {
		if err := copyCheckpointFile(ctx, checkpoint, target, expected); err != nil {
			return err
		}
	}
	return errors.Join(syncDirectory(target), syncDirectory(filepath.Dir(target)))
}

func copyCheckpointFile(ctx context.Context, source, target string, expected File) (err error) {
	input, err := openRegular(filepath.Join(source, expected.Name))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	output, err := os.OpenFile(filepath.Join(target, expected.Name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create restore file %s: %w", expected.Name, err)
	}
	defer func() { err = errors.Join(err, output.Close()) }()
	actual, err := hashFile(ctx, input, output)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("checkpoint file %q changed during restore", expected.Name)
	}
	if err := output.Sync(); err != nil {
		return fmt.Errorf("sync restore file %s: %w", expected.Name, err)
	}
	return nil
}
