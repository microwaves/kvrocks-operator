package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"
)

// The checkpoint protocol is validated against this release. Supporting another
// version requires real-engine capture and restore tests, not a runtime override.
const supportedKvrocksVersion = "2.17.0"

var ErrIndeterminate = errors.New("checkpoint outcome is unknown; retry observation of the same request, not BGSAVE")

type Server struct {
	Version        string
	ProcessID      int64
	Saving         bool
	Loading        bool
	LastSaveStatus string
	Config         map[string]string
}

// Database mutations must use the inspected connection, without reconnection or retries.
type Database interface {
	Inspect(context.Context) (Server, error)
	SetBackupDirectory(context.Context, string) error
	BGSave(context.Context) error
}

type Capturer struct {
	DataDir  string
	Database Database
}

type intent struct {
	Request         Request   `json:"request"`
	ServerProcessID int64     `json:"serverProcessID"`
	ServerDirectory string    `json:"serverDirectory"`
	StartedAt       time.Time `json:"startedAt"`
}

// Capture serializes workers sharing one POSIX filesystem and publishes a local
// artifact. An active journal fences later requests across worker crashes.
func (c Capturer) Capture(ctx context.Context, request Request) (artifact Artifact, err error) {
	if err := request.validate(); err != nil {
		return Artifact{}, err
	}
	if !filepath.IsAbs(c.DataDir) {
		return Artifact{}, fmt.Errorf("data directory must be absolute: %q", c.DataDir)
	}
	root := filepath.Join(c.DataDir, "operator-backup")
	if err := os.MkdirAll(root, 0700); err != nil {
		return Artifact{}, err
	}
	if err := syncDirectory(c.DataDir); err != nil {
		return Artifact{}, err
	}
	guard, err := lock(ctx, root)
	if err != nil {
		return Artifact{}, err
	}
	defer func() { err = errors.Join(err, guard.Close()) }()
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	return c.captureLocked(ctx, root, request)
}

func (c Capturer) captureLocked(ctx context.Context, root string, request Request) (Artifact, error) {
	var active string
	if err := readJSON(filepath.Join(root, "active.json"), &active); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Artifact{}, err
	}
	if active != "" && active != request.ID {
		return Artifact{}, fmt.Errorf("backup %s is still active; resume it before starting %s", active, request.ID)
	}

	directory := filepath.Join(root, request.ID)
	artifact, err := loadArtifact(ctx, directory, request)
	if err != nil {
		return Artifact{}, err
	}
	if artifact != nil {
		return *artifact, clearActive(root, active)
	}

	operation, err := loadIntent(directory, request)
	if err != nil {
		return Artifact{}, err
	}
	if operation == nil {
		if active != "" {
			return Artifact{}, fmt.Errorf("active backup %s is missing its intent; refusing to recapture", active)
		}
		operation, err = c.startCheckpoint(ctx, root, request)
		if err != nil {
			return Artifact{}, err
		}
	} else if active == "" {
		// A worker may have died between publishing intent and the active record.
		// Restore the fence, but never resend the possibly accepted BGSAVE.
		if err := publishJSON(filepath.Join(root, "active.json"), request.ID); err != nil {
			return Artifact{}, err
		}
	}
	if err := c.protectCheckpoint(ctx, directory, *operation); err != nil {
		return Artifact{}, err
	}
	completed, err := publishArtifact(ctx, directory, *operation)
	if err != nil {
		return Artifact{}, err
	}
	return completed, clearActive(root, request.ID)
}

// A nil intent means the request has not been recorded. Malformed or mismatched
// metadata is an error, never permission to start another save.
func loadIntent(directory string, request Request) (*intent, error) {
	var operation intent
	err := readJSON(filepath.Join(directory, "intent.json"), &operation)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if operation.Request != request {
		return nil, fmt.Errorf("backup %s belongs to a different source or volume", request.ID)
	}
	return &operation, nil
}

func (c Capturer) startCheckpoint(ctx context.Context, root string, request Request) (*intent, error) {
	if c.Database == nil {
		return nil, fmt.Errorf("starting checkpoint %s requires a database session", request.ID)
	}
	server, err := c.Database.Inspect(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect Kvrocks before checkpoint: %w", err)
	}
	if err := validateServer(server, request.Source); err != nil {
		return nil, err
	}
	if server.Saving || server.Loading {
		return nil, fmt.Errorf("Kvrocks is already saving or loading; checkpoint was not requested")
	}

	directory := filepath.Join(root, request.ID)
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, fmt.Errorf("create operation directory %s: %w", directory, err)
	}
	operation := intent{
		Request:         request,
		ServerProcessID: server.ProcessID,
		ServerDirectory: path.Join(server.Config["dir"], "operator-backup", request.ID, "incoming"),
		StartedAt:       time.Now().UTC(),
	}
	if err := publishJSON(filepath.Join(directory, "intent.json"), operation); err != nil {
		return nil, err
	}
	if err := publishJSON(filepath.Join(root, "active.json"), request.ID); err != nil {
		return nil, err
	}

	// Both records are durable before either mutation. Only this new-request path
	// can send BGSAVE; resuming an intent only observes the original checkpoint.
	if err := c.Database.SetBackupDirectory(ctx, operation.ServerDirectory); err != nil {
		return nil, fmt.Errorf("set checkpoint directory for %s: %w", request.ID, errors.Join(ErrIndeterminate, err))
	}
	if err := c.Database.BGSave(ctx); err != nil {
		return nil, fmt.Errorf("request checkpoint %s: %w", request.ID, errors.Join(ErrIndeterminate, err))
	}
	return &operation, nil
}

func validateServer(server Server, source Source) error {
	if server.Version != supportedKvrocksVersion {
		return fmt.Errorf("checkpoint requires Kvrocks %s; got %q", supportedKvrocksVersion, server.Version)
	}
	if server.ProcessID <= 0 {
		return fmt.Errorf("checkpoint requires a positive Kvrocks process ID; got %d", server.ProcessID)
	}
	required := []struct {
		key   string
		value string
	}{
		{"cluster-enabled", "no"},
		{"repl-namespace-enabled", "yes"},
		{"rocksdb.write_options.sync", "yes"},
		{"rocksdb.write_options.disable_wal", "no"},
		{"bgsave-cron", ""},
		{"max-backup-to-keep", "1"},
		{"max-backup-keep-hours", "0"},
		{"purge-backup-on-fullsync", "no"},
		{"db-name", source.Namespace + "/" + source.Name},
	}
	for _, setting := range required {
		actual, found := server.Config[setting.key]
		if !found {
			return fmt.Errorf("Kvrocks configuration is missing %s", setting.key)
		}
		if actual != setting.value {
			return fmt.Errorf("checkpoint requires %s=%q; got %q", setting.key, setting.value, actual)
		}
	}
	if !path.IsAbs(server.Config["dir"]) {
		return fmt.Errorf("Kvrocks dir must be absolute: %q", server.Config["dir"])
	}
	return nil
}

func (c Capturer) protectCheckpoint(ctx context.Context, directory string, operation intent) error {
	protected, err := directoryExists(filepath.Join(directory, "checkpoint"))
	if err != nil {
		return err
	}
	if protected {
		return nil
	}
	if c.Database == nil {
		return fmt.Errorf("unresolved checkpoint %s requires a database session", operation.Request.ID)
	}
	for {
		server, err := c.Database.Inspect(ctx)
		if err != nil {
			return fmt.Errorf("observe checkpoint %s: %w", operation.Request.ID, err)
		}
		if err := validateCheckpointObservation(server, operation); err != nil {
			return err
		}
		if server.Saving {
			if err := pause(ctx); err != nil {
				return fmt.Errorf("wait for checkpoint %s: %w", operation.Request.ID, err)
			}
			continue
		}
		return protectCompletedCheckpoint(directory, operation.Request.ID, server.LastSaveStatus)
	}
}

func validateCheckpointObservation(server Server, operation intent) error {
	if server.ProcessID != operation.ServerProcessID {
		return fmt.Errorf("Kvrocks process changed before checkpoint protection: %w", ErrIndeterminate)
	}
	if err := validateServer(server, operation.Request.Source); err != nil {
		return err
	}
	if server.Config["backup-dir"] != operation.ServerDirectory {
		return fmt.Errorf("checkpoint directory changed during backup %s: %w", operation.Request.ID, ErrIndeterminate)
	}
	return nil
}

func protectCompletedCheckpoint(directory, requestID, lastSaveStatus string) error {
	incoming := filepath.Join(directory, "incoming")
	exists, err := directoryExists(incoming)
	if err != nil {
		return err
	}
	if !exists || lastSaveStatus != "ok" {
		return fmt.Errorf("backup %s has no completed checkpoint (last save %q): %w", requestID, lastSaveStatus, ErrIndeterminate)
	}
	if err := os.Rename(incoming, filepath.Join(directory, "checkpoint")); err != nil {
		return fmt.Errorf("protect checkpoint from replacement: %w", err)
	}
	return syncDirectory(directory)
}

func publishArtifact(ctx context.Context, directory string, operation intent) (Artifact, error) {
	checkpoint := filepath.Join(directory, "checkpoint")
	names, err := checkpointFiles(checkpoint)
	if err != nil {
		return Artifact{}, err
	}
	capturedAt, err := loadOrRecordCaptureTime(directory)
	if err != nil {
		return Artifact{}, err
	}
	manifest := Manifest{
		Format:          artifactFormat,
		Request:         operation.Request,
		KvrocksVersion:  supportedKvrocksVersion,
		Encoding:        "standalone",
		ServerProcessID: operation.ServerProcessID,
		StartedAt:       operation.StartedAt,
		CapturedAt:      capturedAt,
	}
	for _, name := range names {
		file, err := syncAndHashFile(ctx, filepath.Join(checkpoint, name))
		if err != nil {
			return Artifact{}, err
		}
		manifest.Files = append(manifest.Files, file)
	}
	if err := manifest.validate(); err != nil {
		return Artifact{}, err
	}
	if err := syncDirectory(checkpoint); err != nil {
		return Artifact{}, err
	}
	if err := publishJSON(filepath.Join(directory, "manifest.json"), manifest); err != nil {
		return Artifact{}, err
	}
	artifact, err := loadArtifact(ctx, directory, operation.Request)
	if err != nil {
		return Artifact{}, err
	}
	if artifact == nil {
		return Artifact{}, fmt.Errorf("published manifest for backup %s is missing", operation.Request.ID)
	}
	return *artifact, nil
}

// Persisting this time makes the manifest bytes stable across interrupted hashing.
func loadOrRecordCaptureTime(directory string) (time.Time, error) {
	name := filepath.Join(directory, "captured.json")
	var capturedAt time.Time
	err := readJSON(name, &capturedAt)
	if err == nil {
		return capturedAt, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return time.Time{}, err
	}
	capturedAt = time.Now().UTC()
	if err := publishJSON(name, capturedAt); err != nil {
		return time.Time{}, err
	}
	return capturedAt, nil
}

// Only an absent manifest returns nil without an error. Once a manifest exists,
// missing checkpoint files are corruption and must never allow a new save.
func loadArtifact(ctx context.Context, directory string, request Request) (*Artifact, error) {
	data, err := readDocument(filepath.Join(directory, "manifest.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	artifact, err := verifyArtifact(ctx, directory, data, manifestDigest(data))
	if err != nil {
		return nil, fmt.Errorf("committed backup %s is invalid: %w", request.ID, err)
	}
	if artifact.Manifest.Request != request {
		return nil, fmt.Errorf("backup %s belongs to a different source or volume", request.ID)
	}
	return &artifact, nil
}

func clearActive(root, active string) error {
	if active == "" {
		return nil
	}
	if err := os.Remove(filepath.Join(root, "active.json")); err != nil {
		return err
	}
	return syncDirectory(root)
}
