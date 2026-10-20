//go:build checkpoint

package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestKvrocksCheckpointRoundTrip(t *testing.T) {
	server := startKvrocks(t, t.TempDir(), "")
	expiry := seedCheckpointData(t, server)
	artifact := captureUnderConcurrentWrites(t, server)

	containsWAL := false
	for _, file := range artifact.Manifest.Files {
		if strings.HasSuffix(file.Name, ".log") {
			containsWAL = true
			break
		}
	}
	if !containsWAL {
		t.Fatal("small unflushed database checkpoint unexpectedly omitted WAL; fixture must exercise WAL recovery")
	}
	assertCheckpointSurvivesSourceChanges(t, server, artifact)

	restoreDirectory := t.TempDir()
	target := filepath.Join(restoreDirectory, "db")
	if err := Materialize(t.Context(), artifact.Directory, artifact.SHA256, target); err != nil {
		t.Fatal(err)
	}
	restored := startKvrocks(t, restoreDirectory, "")
	assertRestoredData(t, restored, expiry)
	if err := Materialize(t.Context(), artifact.Directory, artifact.SHA256, target); err == nil {
		t.Fatal("overwrote a running restored database")
	}
}

func seedCheckpointData(t *testing.T, server *kvrocksProcess) int64 {
	t.Helper()
	client := server.database.client
	ctx := t.Context()
	for _, command := range [][]any{
		{"SET", "marker", "before"},
		{"HSET", "hash", "field", "value"},
		{"RPUSH", "list", "first", "second"},
		{"SADD", "set", "member"},
		{"ZADD", "sorted", "42", "member"},
		{"XADD", "stream", "1-0", "field", "value"},
	} {
		if err := client.Do(ctx, command...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	expiry := time.Now().Add(time.Minute).UnixMilli()
	if err := client.Do(ctx, "SET", "expires", "value", "PXAT", expiry).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(ctx, "NAMESPACE", "ADD", "tenant", "integration-tenant-token").Err(); err != nil {
		t.Fatal(err)
	}
	tenant := NewRedis(server.address, "integration-tenant-token")
	t.Cleanup(func() {
		if err := tenant.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := tenant.client.Set(ctx, "private", "namespaced", 0).Err(); err != nil {
		t.Fatal(err)
	}
	return expiry
}

func captureUnderConcurrentWrites(t *testing.T, server *kvrocksProcess) Artifact {
	t.Helper()
	ctx := t.Context()
	client := server.database.client
	writerContext, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	writerReady := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		for sequence := 0; ; sequence++ {
			if err := client.MSet(writerContext, "pair-a", sequence, "pair-b", sequence).Err(); err != nil {
				writerDone <- err
				return
			}
			if sequence == 10 {
				close(writerReady)
			}
		}
	}()
	select {
	case <-writerReady:
	case err := <-writerDone:
		t.Fatalf("concurrent writer failed before checkpoint: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	capturer := Capturer{DataDir: server.directory, Database: server.database}
	artifact, err := capturer.Capture(ctx, testRequest)
	stopWriter()
	if writerErr := <-writerDone; !errors.Is(writerErr, context.Canceled) {
		t.Fatalf("concurrent writer failed: %v", writerErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("captured Kvrocks %s: %d files; manifest SHA-256 %s", artifact.Manifest.KvrocksVersion, len(artifact.Manifest.Files), artifact.SHA256)
	return artifact
}

func assertCheckpointSurvivesSourceChanges(t *testing.T, server *kvrocksProcess, artifact Artifact) {
	t.Helper()
	ctx := t.Context()
	client := server.database.client
	capturer := Capturer{DataDir: server.directory, Database: server.database}
	if err := client.Set(ctx, "marker", "after", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "later", "absent-from-checkpoint", 0).Err(); err != nil {
		t.Fatal(err)
	}
	other := testRequest
	other.ID = "backup-002"
	if _, err := capturer.Capture(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(ctx, "FLUSHBACKUP").Err(); err != nil {
		t.Fatal(err)
	}
	server.stop(t)
	if _, err := Verify(ctx, artifact.Directory, artifact.SHA256); err != nil {
		t.Fatalf("later backup/purge/shutdown changed artifact: %v", err)
	}
	restarted := startKvrocks(t, server.directory, server.password)
	if value, err := restarted.database.client.Get(ctx, "marker").Result(); err != nil || value != "after" {
		t.Fatalf("source restart lost later writes: %q, %v", value, err)
	}
}

func assertRestoredData(t *testing.T, restored *kvrocksProcess, expiry int64) {
	t.Helper()
	ctx := t.Context()
	pairA, err := restored.database.client.Get(ctx, "pair-a").Result()
	if err != nil {
		t.Fatal(err)
	}
	pairB, err := restored.database.client.Get(ctx, "pair-b").Result()
	if err != nil || pairA != pairB {
		t.Fatalf("checkpoint split an atomic MSET: %q / %q, %v", pairA, pairB, err)
	}
	for _, check := range []struct {
		args []any
		want string
	}{
		{[]any{"GET", "marker"}, "before"},
		{[]any{"HGET", "hash", "field"}, "value"},
		{[]any{"LINDEX", "list", "1"}, "second"},
		{[]any{"SISMEMBER", "set", "member"}, "1"},
		{[]any{"ZSCORE", "sorted", "member"}, "42"},
		{[]any{"XLEN", "stream"}, "1"},
		{[]any{"PEXPIRETIME", "expires"}, fmt.Sprint(expiry)},
		{[]any{"EXISTS", "later"}, "0"},
	} {
		value, err := restored.database.client.Do(ctx, check.args...).Result()
		if err != nil || fmt.Sprint(value) != check.want {
			t.Fatalf("restored %v = %v, %v; want %s", check.args, value, err, check.want)
		}
	}
	restoredTenant := NewRedis(restored.address, "integration-tenant-token")
	t.Cleanup(func() {
		if err := restoredTenant.Close(); err != nil {
			t.Error(err)
		}
	})
	if value, err := restoredTenant.client.Get(ctx, "private").Result(); err != nil || value != "namespaced" {
		t.Fatalf("namespace data/token not restored: %q, %v", value, err)
	}
}

func TestKvrocksWorkerCrashRecovery(t *testing.T) {
	for _, point := range []string{"after-save", "before-save"} {
		t.Run(point, func(t *testing.T) {
			server := startKvrocks(t, t.TempDir(), "")
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCheckpointWorkerProcess$", "-test.v")
			command.Env = append(os.Environ(), "CHECKPOINT_CRASH="+point, "CHECKPOINT_DATA="+server.directory, "CHECKPOINT_ADDRESS="+server.address)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 23 {
				t.Fatalf("worker did not crash at %s: %v\n%s", point, err, output)
			}
			capturer := Capturer{DataDir: server.directory, Database: server.database}
			if point == "after-save" {
				artifact, err := capturer.Capture(t.Context(), testRequest)
				if err != nil {
					t.Fatal(err)
				}
				again, err := capturer.Capture(t.Context(), testRequest)
				if err != nil || again.SHA256 != artifact.SHA256 {
					t.Fatalf("worker crash did not preserve one artifact: %v", err)
				}
				return
			}
			if _, err := capturer.Capture(t.Context(), testRequest); !errors.Is(err, ErrIndeterminate) {
				t.Fatalf("reissued an ambiguous command: %v", err)
			}
			server.stop(t)
			server = startKvrocks(t, server.directory, server.password)
			capturer.Database = server.database
			if _, err := capturer.Capture(t.Context(), testRequest); !errors.Is(err, ErrIndeterminate) {
				t.Fatalf("did not preserve uncertain operation across restart: %v", err)
			}
			next := testRequest
			next.ID = "after-restart"
			if _, err := capturer.Capture(t.Context(), next); err == nil {
				t.Fatal("server restart unsafely released operation fence")
			}
		})
	}
}

type crashDatabase struct {
	*Redis
	point string
}

func (d crashDatabase) BGSave(ctx context.Context) error {
	if d.point == "before-save" {
		os.Exit(23)
	}
	if err := d.Redis.BGSave(ctx); err != nil {
		return err
	}
	os.Exit(23)
	return nil
}

func TestCheckpointWorkerProcess(t *testing.T) {
	point := os.Getenv("CHECKPOINT_CRASH")
	if point == "" {
		t.Skip("subprocess crash fixture")
	}
	directory := os.Getenv("CHECKPOINT_DATA")
	password, err := os.ReadFile(filepath.Join(directory, "test-password"))
	if err != nil {
		t.Fatal(err)
	}
	database := NewRedis(os.Getenv("CHECKPOINT_ADDRESS"), string(password))
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	var session Database = database
	if point != "capture" {
		session = crashDatabase{Redis: database, point: point}
	}
	capturer := Capturer{DataDir: directory, Database: session}
	artifact, err := capturer.Capture(t.Context(), testRequest)
	if err != nil {
		t.Fatal(err)
	}
	if point == "capture" {
		t.Logf("artifact: %s", artifact.SHA256)
		return
	}
	t.Fatal("worker unexpectedly survived crash injection")
}

func TestKvrocksDuplicateWorkerProcesses(t *testing.T) {
	server := startKvrocks(t, t.TempDir(), "")
	outputs := make(chan string, 2)
	for range 2 {
		go func() {
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCheckpointWorkerProcess$", "-test.v")
			command.Env = append(os.Environ(), "CHECKPOINT_CRASH=capture", "CHECKPOINT_DATA="+server.directory, "CHECKPOINT_ADDRESS="+server.address)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Errorf("duplicate worker failed: %v\n%s", err, output)
			}
			outputs <- string(output)
		}()
	}
	first, second := <-outputs, <-outputs
	capturer := Capturer{DataDir: server.directory}
	artifact, err := capturer.Capture(t.Context(), testRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{first, second} {
		if !strings.Contains(output, "artifact: "+artifact.SHA256) {
			t.Fatalf("worker returned a different artifact:\n%s", output)
		}
	}
}

type kvrocksProcess struct {
	directory string
	address   string
	password  string
	database  *Redis
	command   *exec.Cmd
	done      chan error
	stopped   bool
}

func startKvrocks(t *testing.T, directory, password string) *kvrocksProcess {
	t.Helper()
	port := kvrocksTestPort(t, directory)
	if password == "" {
		bytes := make([]byte, 24)
		if _, err := rand.Read(bytes); err != nil {
			t.Fatal(err)
		}
		password = hex.EncodeToString(bytes)
	}
	writeTestFile(t, filepath.Join(directory, "test-password"), []byte(password))
	configName := writeKvrocksTestConfig(t, directory, password, port)
	log, err := os.OpenFile(filepath.Join(directory, "kvrocks.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("kvrocks", "-c", configName)
	command.Stdout, command.Stderr = log, log
	startErr := command.Start()
	if err := errors.Join(startErr, log.Close()); err != nil {
		t.Fatal(err)
	}
	server := &kvrocksProcess{
		directory: directory,
		address:   fmt.Sprintf("127.0.0.1:%d", port),
		password:  password,
		command:   command,
		done:      make(chan error, 1),
	}
	go func() { server.done <- command.Wait() }()
	server.database = NewRedis(server.address, password)
	t.Cleanup(func() {
		server.stop(t)
		if err := server.database.Close(); err != nil {
			t.Error(err)
		}
	})
	server.waitUntilReady(t)
	return server
}

func kvrocksTestPort(t *testing.T, directory string) int {
	t.Helper()
	var port int
	portData, err := os.ReadFile(filepath.Join(directory, "test-port"))
	if errors.Is(err, os.ErrNotExist) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port = listener.Addr().(*net.TCPAddr).Port
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, filepath.Join(directory, "test-port"), []byte(strconv.Itoa(port)))
	} else if err != nil {
		t.Fatal(err)
	} else {
		port, err = strconv.Atoi(string(portData))
		if err != nil {
			t.Fatal(err)
		}
	}
	return port
}

func writeKvrocksTestConfig(t *testing.T, directory, password string, port int) string {
	t.Helper()
	configuration := fmt.Sprintf(`bind 127.0.0.1
port %d
daemonize no
dir %s
pidfile %s/kvrocks.pid
log-dir stdout
db-name test/database
requirepass %s
cluster-enabled no
repl-namespace-enabled yes
rocksdb.write_options.sync yes
rocksdb.write_options.disable_wal no
max-backup-to-keep 1
max-backup-keep-hours 0
purge-backup-on-fullsync no
`, port, directory, directory, password)
	configName := filepath.Join(directory, "kvrocks.conf")
	writeTestFile(t, configName, []byte(configuration))
	return configName
}

func (s *kvrocksProcess) waitUntilReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.database.client.Ping(t.Context()).Err(); err == nil {
			return
		}
		if err := pause(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	data, readErr := os.ReadFile(filepath.Join(s.directory, "kvrocks.log"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	t.Fatalf("Kvrocks failed to start:\n%s", data)
}

func (s *kvrocksProcess) stop(t *testing.T) {
	t.Helper()
	if s.stopped {
		return
	}
	s.stopped = true
	if err := s.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Error(err)
	}
	select {
	case err := <-s.done:
		if err != nil {
			t.Errorf("Kvrocks exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		if err := s.command.Process.Kill(); err != nil {
			t.Error(err)
		}
		err := <-s.done
		t.Errorf("Kvrocks required forced shutdown: %v", err)
	}
}

func TestKvrocksSessionDoesNotReconnectAcrossRestart(t *testing.T) {
	server := startKvrocks(t, t.TempDir(), "")
	original := server.database
	before, err := original.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	server.stop(t)
	replacement := startKvrocks(t, server.directory, server.password)
	after, err := replacement.database.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if before.ProcessID == after.ProcessID {
		t.Fatal("fixture did not restart Kvrocks")
	}
	if _, err := original.Inspect(t.Context()); err == nil {
		t.Fatal("session silently reconnected to a new process")
	}
	if err := original.BGSave(t.Context()); err == nil {
		t.Fatal("old session sent BGSAVE to a new process")
	}
}
