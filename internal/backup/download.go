package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type downloadIntent struct {
	ManifestKey string `json:"manifestKey"`
	SHA256      string `json:"sha256"`
}

// Download resumes into a directory reserved for one artifact. The manifest is
// published only after every file is verified and durable; it marks completion.
func (t Transfer) Download(ctx context.Context, manifestKey, digest, directory string) (artifact Artifact, err error) {
	if err := t.validate(manifestKey, digest); err != nil {
		return Artifact{}, err
	}
	if !filepath.IsAbs(directory) {
		return Artifact{}, fmt.Errorf("download directory must be absolute: %q", directory)
	}
	if err := createDownloadDirectory(directory); err != nil {
		return Artifact{}, err
	}
	guard, err := lock(ctx, directory)
	if err != nil {
		return Artifact{}, err
	}
	defer func() { err = errors.Join(err, guard.Close()) }()
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	if err := reserveDownload(directory, downloadIntent{ManifestKey: manifestKey, SHA256: digest}); err != nil {
		return Artifact{}, err
	}

	localManifest := filepath.Join(directory, "manifest.json")
	data, err := readDocument(localManifest)
	if err == nil {
		artifact, err := verifyArtifact(ctx, directory, data, digest)
		if err != nil {
			return Artifact{}, err
		}
		return artifact, syncDirectory(directory)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Artifact{}, err
	}
	data, err = t.readManifest(ctx, manifestKey)
	if err != nil {
		return Artifact{}, err
	}
	manifest, err := transferManifest(data, manifestKey, digest)
	if err != nil {
		return Artifact{}, err
	}
	checkpoint := filepath.Join(directory, "checkpoint")
	if err := createDownloadDirectory(checkpoint); err != nil {
		return Artifact{}, err
	}
	for _, file := range manifest.Files {
		if err := t.downloadFile(ctx, directory, checkpointKey(manifestKey, file.Name), file); err != nil {
			return Artifact{}, err
		}
	}
	artifact, err = verifyArtifact(ctx, directory, data, digest)
	if err != nil {
		return Artifact{}, err
	}
	// A previous worker may have linked the last file but exited before syncing
	// the directory. Reused files need the same publication barrier as new files.
	if err := syncDirectory(checkpoint); err != nil {
		return Artifact{}, err
	}
	if err := publishDocument(localManifest, data); err != nil {
		return Artifact{}, err
	}
	return artifact, nil
}

func createDownloadDirectory(directory string) error {
	exists, err := directoryExists(directory)
	if err != nil {
		return err
	}
	if !exists {
		if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create download directory %s: %w", directory, err)
		}
		if _, err := directoryExists(directory); err != nil {
			return err
		}
	}
	return syncDirectory(filepath.Dir(directory))
}

func reserveDownload(directory string, expected downloadIntent) error {
	name := filepath.Join(directory, "download.json")
	var existing downloadIntent
	err := readJSON(name, &existing)
	if err == nil {
		if existing != expected {
			return fmt.Errorf("download directory %s belongs to a different manifest key or checksum", directory)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != ".lock" {
			return fmt.Errorf("download directory %s is not empty and has no download intent", directory)
		}
	}
	return publishJSON(name, expected)
}

func (t Transfer) downloadFile(ctx context.Context, directory, key string, expected File) (err error) {
	checkpoint := filepath.Join(directory, "checkpoint")
	target := filepath.Join(checkpoint, expected.Name)
	if err := verifyCheckpointFile(ctx, target, expected); !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Only this journal's locked directory owns the temporary file. A crash may
	// leave it behind, but it is never treated as a completed checkpoint file.
	temporary := filepath.Join(directory, "download.tmp")
	if err := os.Remove(temporary); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove interrupted download %s: %w", temporary, err)
	}
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create download temporary file %s: %w", temporary, err)
	}
	defer func() { err = errors.Join(err, os.Remove(temporary)) }()
	if err := t.verifyObject(ctx, key, expected, file); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("sync downloaded file %q: %w", key, err)
	}
	if err := os.Link(temporary, target); err != nil {
		return fmt.Errorf("publish downloaded file %s without replacing it: %w", target, err)
	}
	return syncDirectory(checkpoint)
}
