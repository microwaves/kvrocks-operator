package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var testRequest = Request{
	ID: "backup-001",
	Source: Source{
		Namespace: "test",
		Name:      "database",
		UID:       "database-uid",
		VolumeUID: "volume-uid",
	},
}

type fakeDatabase struct {
	server       Server
	saves        int
	beforeSave   func() error
	afterSave    func() error
	afterInspect func()
}

func newFake(directory string) *fakeDatabase {
	return &fakeDatabase{
		server: Server{
			Version:        supportedKvrocksVersion,
			ProcessID:      101,
			LastSaveStatus: "ok",
			Config: map[string]string{
				"dir":                               directory,
				"db-name":                           "test/database",
				"backup-dir":                        filepath.Join(directory, "backup"),
				"cluster-enabled":                   "no",
				"repl-namespace-enabled":            "yes",
				"rocksdb.write_options.sync":        "yes",
				"rocksdb.write_options.disable_wal": "no",
				"bgsave-cron":                       "",
				"max-backup-to-keep":                "1",
				"max-backup-keep-hours":             "0",
				"purge-backup-on-fullsync":          "no",
			},
		},
	}
}

func (f *fakeDatabase) Inspect(context.Context) (Server, error) {
	if f.afterInspect != nil {
		f.afterInspect()
	}
	return f.server, nil
}

func (f *fakeDatabase) SetBackupDirectory(_ context.Context, directory string) error {
	f.server.Config["backup-dir"] = directory
	return nil
}

func (f *fakeDatabase) BGSave(context.Context) error {
	f.saves++
	if f.beforeSave != nil {
		return f.beforeSave()
	}
	directory := f.server.Config["backup-dir"]
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	files := map[string]string{
		"CURRENT":         "MANIFEST-000001\n",
		"MANIFEST-000001": "manifest",
		"000001.log":      "unflushed data",
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0600); err != nil {
			return err
		}
	}
	if f.afterSave != nil {
		return f.afterSave()
	}
	return nil
}

func fixture(t *testing.T) (Capturer, *fakeDatabase) {
	t.Helper()
	directory := t.TempDir()
	database := newFake(directory)
	return Capturer{DataDir: directory, Database: database}, database
}

func TestConcurrentCaptureIsOneOperation(t *testing.T) {
	capturer, database := fixture(t)
	var group sync.WaitGroup
	results := make(chan Artifact, 8)
	for range cap(results) {
		group.Go(func() {
			artifact, err := capturer.Capture(t.Context(), testRequest)
			if err != nil {
				t.Error(err)
				return
			}
			results <- artifact
		})
	}
	group.Wait()
	close(results)
	var digest string
	for artifact := range results {
		if digest != "" && digest != artifact.SHA256 {
			t.Fatal("duplicate workers published different artifacts")
		}
		digest = artifact.SHA256
	}
	if database.saves != 1 || digest == "" {
		t.Fatalf("saved %d times; digest %q", database.saves, digest)
	}
	request := testRequest
	request.Source.VolumeUID = "replaced-volume"
	if _, err := capturer.Capture(t.Context(), request); err == nil {
		t.Fatal("reused an operation for a different volume")
	}
}

func TestLostSaveResponseRecoversOriginalCheckpoint(t *testing.T) {
	capturer, database := fixture(t)
	database.afterSave = func() error { return errors.New("response connection lost") }
	if _, err := capturer.Capture(t.Context(), testRequest); !errors.Is(err, ErrIndeterminate) {
		t.Fatalf("expected indeterminate result, got %v", err)
	}
	artifact, err := capturer.Capture(t.Context(), testRequest)
	if err != nil {
		t.Fatal(err)
	}
	if database.saves != 1 || len(artifact.Manifest.Files) != 3 {
		t.Fatalf("recovery issued another save or omitted checkpoint files: %d, %+v", database.saves, artifact.Manifest.Files)
	}
}

func TestUnsentRequestRemainsFencedAcrossServerRestart(t *testing.T) {
	capturer, database := fixture(t)
	database.beforeSave = func() error { return errors.New("connection lost before sending command") }
	for range 2 {
		if _, err := capturer.Capture(t.Context(), testRequest); !errors.Is(err, ErrIndeterminate) {
			t.Fatalf("expected indeterminate result, got %v", err)
		}
	}
	other := testRequest
	other.ID = "backup-002"
	if _, err := capturer.Capture(t.Context(), other); err == nil || !strings.Contains(err.Error(), "still active") {
		t.Fatalf("new operation bypassed unresolved operation: %v", err)
	}
	if database.saves != 1 {
		t.Fatalf("ambiguous command was retried %d times", database.saves)
	}
	database.server.ProcessID = 202
	if _, err := capturer.Capture(t.Context(), testRequest); !errors.Is(err, ErrIndeterminate) {
		t.Fatalf("expected unresolved process change, got %v", err)
	}
	database.beforeSave = nil
	if _, err := capturer.Capture(t.Context(), other); err == nil {
		t.Fatal("a PID change released an unresolved operation")
	}
}

func TestResumeAfterCheckpointRenameWithoutDatabase(t *testing.T) {
	capturer, database := fixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	database.afterSave = func() error {
		database.afterInspect = cancel
		return nil
	}
	if _, err := capturer.Capture(ctx, testRequest); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled hashing after checkpoint was protected, got %v", err)
	}
	capturer.Database = nil
	artifact, err := capturer.Capture(t.Context(), testRequest)
	if err != nil {
		t.Fatal(err)
	}
	again, err := capturer.Capture(t.Context(), testRequest)
	if err != nil || artifact.SHA256 != again.SHA256 {
		t.Fatalf("offline resume changed artifact: %v", err)
	}
}

func TestUnsafeServerConfigurationDoesNotStartSave(t *testing.T) {
	for key, value := range map[string]string{
		"max-backup-keep-hours":             "24",
		"max-backup-to-keep":                "0",
		"cluster-enabled":                   "yes",
		"bgsave-cron":                       "* * * * *",
		"purge-backup-on-fullsync":          "yes",
		"db-name":                           "another/database",
		"rocksdb.write_options.disable_wal": "yes",
		"rocksdb.write_options.sync":        "no",
		"repl-namespace-enabled":            "no",
	} {
		t.Run(key, func(t *testing.T) {
			capturer, database := fixture(t)
			database.server.Config[key] = value
			if _, err := capturer.Capture(t.Context(), testRequest); err == nil {
				t.Fatal("accepted unsafe configuration")
			}
			if database.saves != 0 {
				t.Fatal("started a save before validation")
			}
		})
	}
}

func TestLockWaitHonorsCancellation(t *testing.T) {
	capturer, _ := fixture(t)
	root := filepath.Join(capturer.DataDir, "operator-backup")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	guard, err := lock(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := guard.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := capturer.Capture(ctx, testRequest); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting for lock did not respect context: %v", err)
	}
}

func TestMissingCommittedFileNeverTriggersNewSave(t *testing.T) {
	for _, missing := range []string{"000001.log", "CURRENT", "MANIFEST-000001"} {
		t.Run(missing, func(t *testing.T) {
			capturer, database := fixture(t)
			artifact, err := capturer.Capture(t.Context(), testRequest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(artifact.Directory, "checkpoint", missing)); err != nil {
				t.Fatal(err)
			}
			if _, err := capturer.Capture(t.Context(), testRequest); err == nil {
				t.Fatal("accepted corrupted committed checkpoint")
			}
			if database.saves != 1 {
				t.Fatal("recaptured an operation whose committed data was missing")
			}
		})
	}
}

func TestCheckpointConfigurationChangeIsFenced(t *testing.T) {
	capturer, database := fixture(t)
	database.afterSave = func() error {
		database.server.Config["backup-dir"] = "/another-directory"
		return nil
	}
	if _, err := capturer.Capture(t.Context(), testRequest); !errors.Is(err, ErrIndeterminate) {
		t.Fatalf("accepted changed backup destination: %v", err)
	}
}

func TestRejectInvalidRequest(t *testing.T) {
	for _, id := range []string{"", ".", "..", "../../db", "/db", "a/b", "a\\b"} {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			capturer, database := fixture(t)
			request := testRequest
			request.ID = id
			if _, err := capturer.Capture(t.Context(), request); err == nil || database.saves != 0 {
				t.Fatalf("unsafe request reached database: %v", err)
			}
		})
	}
}

func TestServerInfoParsing(t *testing.T) {
	const info = "# Server\r\nkvrocks_version:2.17.0\r\nprocess_id:101\r\n" +
		"# Persistence\r\nbgsave_in_progress:0\r\nloading:0\r\nlast_bgsave_status:ok\r\n"
	cases := []struct {
		name        string
		info        string
		wantSaving  bool
		wantLoading bool
		wantError   string
	}{
		{name: "idle", info: info},
		{name: "saving", info: strings.ReplaceAll(info, "bgsave_in_progress:0", "bgsave_in_progress:1"), wantSaving: true},
		{name: "loading", info: strings.ReplaceAll(info, "loading:0", "loading:1"), wantLoading: true},
		{name: "missing save state", info: strings.ReplaceAll(info, "bgsave_in_progress:0\r\n", ""), wantError: "bgsave_in_progress"},
		{name: "unknown save state", info: strings.ReplaceAll(info, "bgsave_in_progress:0", "bgsave_in_progress:yes"), wantError: "bgsave_in_progress"},
		{name: "missing load state", info: strings.ReplaceAll(info, "loading:0\r\n", ""), wantError: "loading"},
		{name: "unknown load state", info: strings.ReplaceAll(info, "loading:0", "loading:2"), wantError: "loading"},
		{name: "missing process ID", info: strings.ReplaceAll(info, "process_id:101\r\n", ""), wantError: "process_id"},
		{name: "zero process ID", info: strings.ReplaceAll(info, "process_id:101", "process_id:0"), wantError: "process_id"},
		{name: "negative process ID", info: strings.ReplaceAll(info, "process_id:101", "process_id:-1"), wantError: "process_id"},
		{name: "invalid process ID", info: strings.ReplaceAll(info, "process_id:101", "process_id:unknown"), wantError: "process_id"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server, err := parseServerInfo(test.info)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("expected error identifying %s, got %v", test.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if server.Version != "2.17.0" || server.ProcessID != 101 || server.LastSaveStatus != "ok" {
				t.Fatalf("lost server metadata: %+v", server)
			}
			if server.Saving != test.wantSaving || server.Loading != test.wantLoading {
				t.Fatalf("incorrect save/loading state: %+v", server)
			}
		})
	}
}

func TestCaptureRejectsUnsupportedOrBusyServer(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Server)
	}{
		{"unsupported version", func(s *Server) { s.Version = "2.18.0" }},
		{"missing version", func(s *Server) { s.Version = "" }},
		{"invalid process ID", func(s *Server) { s.ProcessID = 0 }},
		{"saving", func(s *Server) { s.Saving = true }},
		{"loading", func(s *Server) { s.Loading = true }},
		{"missing required setting", func(s *Server) { delete(s.Config, "bgsave-cron") }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			capturer, database := fixture(t)
			test.change(&database.server)
			originalDirectory := database.server.Config["backup-dir"]
			if _, err := capturer.Capture(t.Context(), testRequest); err == nil {
				t.Fatal("accepted unsupported or busy server")
			}
			if database.saves != 0 || database.server.Config["backup-dir"] != originalDirectory {
				t.Fatal("mutated database before validating server")
			}
		})
	}
}

func TestIncompleteJournalNeverResendsSave(t *testing.T) {
	for _, missing := range []string{"intent", "active"} {
		t.Run(missing, func(t *testing.T) {
			capturer, database := fixture(t)
			database.beforeSave = func() error { return errors.New("connection lost") }
			if _, err := capturer.Capture(t.Context(), testRequest); !errors.Is(err, ErrIndeterminate) {
				t.Fatalf("expected unresolved save, got %v", err)
			}
			root := filepath.Join(capturer.DataDir, "operator-backup")
			missingFile := filepath.Join(root, "active.json")
			if missing == "intent" {
				missingFile = filepath.Join(root, testRequest.ID, "intent.json")
			}
			if err := os.Remove(missingFile); err != nil {
				t.Fatal(err)
			}
			if _, err := capturer.Capture(t.Context(), testRequest); err == nil {
				t.Fatal("accepted an incomplete journal")
			}
			if database.saves != 1 {
				t.Fatal("incomplete journal triggered a second BGSAVE")
			}
			other := testRequest
			other.ID = "backup-002"
			if _, err := capturer.Capture(t.Context(), other); err == nil {
				t.Fatal("unresolved request no longer blocks later requests")
			}
		})
	}
}
