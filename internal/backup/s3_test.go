package backup

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func testS3Client(endpoint string, client *http.Client) *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		HTTPClient:   client,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test-access-key", SecretAccessKey: "test-secret-key"}, nil
		}),
		Retryer: retry.NewStandard(func(options *retry.StandardOptions) {
			options.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		}),
	})
}

func TestS3RetriesRewindUploadBody(t *testing.T) {
	data := []byte("the same bytes on every attempt")
	digest := manifestDigest(data)
	checksum, err := hex.DecodeString(digest)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/backups/object" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("If-None-Match") != "*" || r.Header.Get("X-Amz-Checksum-Sha256") != base64.StdEncoding.EncodeToString(checksum) {
			t.Error("missing conditional create or SHA-256 checksum")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, data) {
			t.Errorf("retry changed upload body: %q, %v", body, err)
		}
		if attempts.Add(1) < 3 {
			s3ErrorResponse(t, w, 503, "SlowDown")
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	store, err := NewS3(testS3Client(server.URL, server.Client()), "backups")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutIfAbsent(t.Context(), "object", bytes.NewReader(data), int64(len(data)), digest); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestS3ErrorAndRetryPolicy(t *testing.T) {
	for _, test := range []struct {
		code     string
		status   int
		attempts int32
		want     error
	}{
		{code: "SlowDown", status: 503, attempts: 3},
		{code: "AccessDenied", status: 403, attempts: 1},
		{code: "NoSuchBucket", status: 404, attempts: 1},
		{code: "NoSuchKey", status: 404, attempts: 1, want: ErrObjectNotFound},
		{code: "PreconditionFailed", status: 412, attempts: 1, want: ErrObjectExists},
	} {
		t.Run(test.code, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				s3ErrorResponse(t, w, test.status, test.code)
			}))
			defer server.Close()
			store, err := NewS3(testS3Client(server.URL, server.Client()), "backups")
			if err != nil {
				t.Fatal(err)
			}
			if test.code == "PreconditionFailed" {
				err = store.PutIfAbsent(t.Context(), "object", strings.NewReader(""), 0, manifestDigest(nil))
			} else {
				var body io.ReadCloser
				body, err = store.Get(t.Context(), "object")
				if body != nil {
					if closeErr := body.Close(); closeErr != nil {
						t.Error(closeErr)
					}
				}
			}
			if err == nil || !strings.Contains(err.Error(), test.code) {
				t.Fatalf("lost concrete S3 error: %v", err)
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
			if test.want == nil && errors.Is(err, ErrObjectNotFound) {
				t.Fatal("treated bucket or permission failure as missing object")
			}
			if attempts.Load() != test.attempts {
				t.Fatalf("got %d attempts, want %d", attempts.Load(), test.attempts)
			}
		})
	}
}

func s3ErrorResponse(t *testing.T, w http.ResponseWriter, status int, code string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if _, err := fmt.Fprintf(w, "<Error><Code>%s</Code><Message>test response</Message></Error>", code); err != nil {
		t.Error(err)
	}
}
