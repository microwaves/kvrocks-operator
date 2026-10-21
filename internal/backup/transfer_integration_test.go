//go:build checkpoint

package backup

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestKvrocksObjectStorageRoundTrip(t *testing.T) {
	store := startObjectStore(t)
	assertConditionalCreate(t, store)
	server := startKvrocks(t, t.TempDir(), "")
	expiry := seedCheckpointData(t, server)
	artifact := captureUnderConcurrentWrites(t, server)
	if err := server.database.client.Set(t.Context(), "marker", "after", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := server.database.client.Set(t.Context(), "later", "absent-from-checkpoint", 0).Err(); err != nil {
		t.Fatal(err)
	}
	transfer := Transfer{Store: store}
	uploadFixture(t, transfer, artifact)

	server.stop(t)
	if err := os.RemoveAll(server.directory); err != nil {
		t.Fatal(err)
	}
	// Recovery now depends entirely on object storage, including upload retries.
	uploadFixture(t, transfer, artifact)
	downloaded, err := transfer.Download(t.Context(), testManifestKey, artifact.SHA256, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	restoreDirectory := t.TempDir()
	if err := Materialize(t.Context(), downloaded.Directory, downloaded.SHA256, filepath.Join(restoreDirectory, "db")); err != nil {
		t.Fatal(err)
	}
	restored := startKvrocks(t, restoreDirectory, "")
	assertRestoredData(t, restored, expiry)
	t.Log("Kvrocks 2.17.0 → SeaweedFS 4.48 over TLS → fresh Kvrocks 2.17.0 passed after deleting the source")
}

func assertConditionalCreate(t *testing.T, store *S3) {
	t.Helper()
	first, second := []byte("original"), []byte("replacement")
	if err := store.PutIfAbsent(t.Context(), "conditional-create", bytes.NewReader(first), int64(len(first)), manifestDigest(first)); err != nil {
		t.Fatal(err)
	}
	if err := store.PutIfAbsent(t.Context(), "conditional-create", bytes.NewReader(second), int64(len(second)), manifestDigest(second)); !errors.Is(err, ErrObjectExists) {
		t.Fatalf("S3 provider did not reject replacement: %v", err)
	}
	body, err := store.Get(t.Context(), "conditional-create")
	if err != nil {
		t.Fatal(err)
	}
	actual, readErr := io.ReadAll(body)
	if err := errors.Join(readErr, body.Close()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, first) {
		t.Fatal("conditional write replaced the original object")
	}
}

func startObjectStore(t *testing.T) *S3 {
	t.Helper()
	directory := t.TempDir()
	port := kvrocksTestPort(t, directory)
	httpClient := objectStoreTLS(t, directory)
	credentials := aws.Credentials{AccessKeyID: randomTestCredential(t), SecretAccessKey: randomTestCredential(t)}
	command := exec.Command("weed", "mini",
		"-dir="+directory, "-ip=127.0.0.1", "-ip.bind=127.0.0.1",
		fmt.Sprintf("-s3.port.https=%d", port),
		"-s3.cert.file="+filepath.Join(directory, "cert.pem"),
		"-s3.key.file="+filepath.Join(directory, "key.pem"),
		"-webdav=false", "-admin.ui=false", "-s3.port.iceberg=0", "-s3.port.lance=0", "-volume.max=4",
	)
	command.Env = append(os.Environ(), "AWS_ACCESS_KEY_ID="+credentials.AccessKeyID, "AWS_SECRET_ACCESS_KEY="+credentials.SecretAccessKey)
	startStoreProcess(t, command, filepath.Join(directory, "seaweedfs.log"))
	client := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(fmt.Sprintf("https://127.0.0.1:%d", port)),
		UsePathStyle: true, HTTPClient: httpClient,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return credentials, nil }),
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("backups")}, boundedRetries)
		if err == nil {
			break
		}
		if pauseErr := pause(ctx); pauseErr != nil {
			t.Fatalf("S3 fixture did not become ready: %v", errors.Join(err, pauseErr))
		}
	}
	store, err := NewS3(client, "backups")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func startStoreProcess(t *testing.T, command *exec.Cmd, logName string) {
	t.Helper()
	log, err := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = log, log
	startErr := command.Start()
	if err := errors.Join(startErr, log.Close()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		if err := command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("S3 fixture exit: %v", err)
			}
		case <-time.After(15 * time.Second):
			if err := command.Process.Kill(); err != nil {
				t.Error(err)
			}
			t.Errorf("S3 fixture required forced shutdown: %v", <-done)
		}
		if t.Failed() {
			data, err := os.ReadFile(logName)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("S3 fixture log:\n%s", data)
		}
	})
}

func randomTestCredential(t *testing.T) string {
	t.Helper()
	data := make([]byte, 24)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data)
}

func objectStoreTLS(t *testing.T, directory string) *http.Client {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "object storage test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	writeTestFile(t, filepath.Join(directory, "cert.pem"), certPEM)
	writeTestFile(t, filepath.Join(directory, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("could not trust generated test certificate")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}
}
