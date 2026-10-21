package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

func lock(ctx context.Context, directory string) (*os.File, error) {
	file, err := os.OpenFile(filepath.Join(directory, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open checkpoint lock: %w", err)
	}
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			return nil, errors.Join(fmt.Errorf("lock checkpoint directory: %w", err), file.Close())
		}
		if err := pause(ctx); err != nil {
			return nil, errors.Join(err, file.Close())
		}
	}
}

func pause(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func syncDirectory(directory string) (err error) {
	file, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open directory %s for synchronization: %w", directory, err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync directory %s: %w", directory, err)
	}
	return nil
}

func publishJSON(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode checkpoint metadata %s: %w", name, err)
	}
	return publishDocument(name, append(data, '\n'))
}

// A synced temporary file is linked into place without replacing an existing result.
func publishDocument(name string, data []byte) (err error) {
	directory := filepath.Dir(name)
	file, err := os.CreateTemp(directory, ".publish-*")
	if err != nil {
		return fmt.Errorf("create temporary metadata file for %s: %w", name, err)
	}
	defer func() { err = errors.Join(err, os.Remove(file.Name())) }()
	if _, err := file.Write(data); err != nil {
		return errors.Join(fmt.Errorf("write checkpoint metadata %s: %w", name, err), file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync checkpoint metadata %s: %w", name, err), file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(file.Name(), name); err != nil {
		return fmt.Errorf("publish %s without replacing it: %w", name, err)
	}
	return syncDirectory(directory)
}

func readDocument(name string) (data []byte, err error) {
	file, err := openRegular(name)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	data, err = io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxManifestBytes {
		return nil, fmt.Errorf("%s exceeds manifest size limit", name)
	}
	return data, nil
}

func readJSON(name string, value any) error {
	data, err := readDocument(name)
	if err != nil {
		return err
	}
	if err := decodeJSON(data, value); err != nil {
		return fmt.Errorf("read checkpoint metadata %s: %w", name, err)
	}
	return nil
}

func decodeJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode checkpoint metadata: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("checkpoint metadata contains trailing JSON or invalid data")
	}
	return nil
}

func openRegular(name string) (*os.File, error) {
	file, err := os.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("%s is not a regular file", name), file.Close())
	}
	return file, nil
}

func directoryExists(name string) (bool, error) {
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("%s must be a directory, not a symlink or file", name)
	}
	return true, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
