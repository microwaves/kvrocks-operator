package backup

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type S3 struct {
	client *s3.Client
	bucket string
}

// The caller configures credentials, region, endpoint and TLS on the SDK client.
func NewS3(client *s3.Client, bucket string) (*S3, error) {
	if client == nil || bucket == "" {
		return nil, fmt.Errorf("S3 object store requires a client and bucket")
	}
	return &S3{client: client, bucket: bucket}, nil
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, boundedRetries)
	if err != nil {
		return nil, s.objectError("get", key, err)
	}
	return result.Body, nil
}

func (s *S3) PutIfAbsent(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string) error {
	if size < 0 || size > maxObjectBytes || !validDigest(digest) {
		return fmt.Errorf("cannot upload object %q: invalid size or SHA-256", key)
	}
	checksum, err := hex.DecodeString(digest)
	if err != nil {
		return fmt.Errorf("decode SHA-256 for object %q: %w", key, err)
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(s.bucket),
		Key:            aws.String(key),
		Body:           body,
		ContentLength:  aws.Int64(size),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(checksum)),
		IfNoneMatch:    aws.String("*"),
	}, boundedRetries)
	if err != nil {
		return s.objectError("put", key, err)
	}
	return nil
}

func boundedRetries(options *s3.Options) {
	options.RetryMaxAttempts = 3
}

func (s *S3) objectError(operation, key string, err error) error {
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "NoSuchKey":
			err = errors.Join(ErrObjectNotFound, err)
		case "PreconditionFailed":
			err = errors.Join(ErrObjectExists, err)
		}
	}
	return fmt.Errorf("S3 %s s3://%s/%s: %w", operation, s.bucket, key, err)
}
