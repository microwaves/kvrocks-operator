package backup

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client     *redis.Client
	connection *redis.Conn
}

func NewRedis(address, password string) *Redis {
	client := redis.NewClient(&redis.Options{
		Addr:                  address,
		Password:              password,
		Protocol:              2,
		MaxRetries:            -1,
		DialTimeout:           5 * time.Second,
		ReadTimeout:           5 * time.Second,
		WriteTimeout:          5 * time.Second,
		ContextTimeoutEnabled: true,
		DisableIdentity:       true,
	})
	// A dedicated connection cannot reconnect and send BGSAVE to a new server
	// process after Inspect observed the previous process.
	return &Redis{client: client, connection: client.Conn()}
}

func (r *Redis) Close() error {
	return errors.Join(r.connection.Close(), r.client.Close())
}

func (r *Redis) Inspect(ctx context.Context) (Server, error) {
	info, err := r.connection.Info(ctx, "all").Result()
	if err != nil {
		return Server{}, fmt.Errorf("read Kvrocks INFO: %w", err)
	}
	server, err := parseServerInfo(info)
	if err != nil {
		return Server{}, err
	}
	server.Config, err = r.readBackupConfig(ctx)
	if err != nil {
		return Server{}, err
	}
	return server, nil
}

func parseServerInfo(info string) (Server, error) {
	fields := make(map[string]string)
	for line := range strings.SplitSeq(info, "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), ":")
		if found {
			fields[key] = value
		}
	}
	saving, err := parseInfoFlag("bgsave_in_progress", fields["bgsave_in_progress"])
	if err != nil {
		return Server{}, err
	}
	loading, err := parseInfoFlag("loading", fields["loading"])
	if err != nil {
		return Server{}, err
	}
	processID, err := strconv.ParseInt(fields["process_id"], 10, 64)
	if err != nil || processID <= 0 {
		return Server{}, fmt.Errorf("Kvrocks INFO has invalid process_id %q", fields["process_id"])
	}
	return Server{
		Version:        fields["kvrocks_version"],
		ProcessID:      processID,
		Saving:         saving,
		Loading:        loading,
		LastSaveStatus: fields["last_bgsave_status"],
	}, nil
}

func parseInfoFlag(name, value string) (bool, error) {
	switch value {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("Kvrocks INFO has invalid %s %q: expected 0 or 1", name, value)
	}
}

func (r *Redis) readBackupConfig(ctx context.Context) (map[string]string, error) {
	keys := []string{
		"dir",
		"db-name",
		"backup-dir",
		"cluster-enabled",
		"repl-namespace-enabled",
		"rocksdb.write_options.sync",
		"rocksdb.write_options.disable_wal",
		"bgsave-cron",
		"max-backup-to-keep",
		"max-backup-keep-hours",
		"purge-backup-on-fullsync",
	}
	config := make(map[string]string, len(keys))
	for _, key := range keys {
		values, err := r.connection.ConfigGet(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("read Kvrocks configuration %s: %w", key, err)
		}
		if value, found := values[key]; found {
			config[key] = value
		}
	}
	return config, nil
}

func (r *Redis) SetBackupDirectory(ctx context.Context, directory string) error {
	return r.connection.ConfigSet(ctx, "backup-dir", directory).Err()
}

func (r *Redis) BGSave(ctx context.Context) error {
	return r.connection.Do(ctx, "BGSAVE").Err()
}
