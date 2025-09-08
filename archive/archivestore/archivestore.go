package archivestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const CName = "node.archive.store"

var log = logger.NewNamed(CName)

var (
	ErrNotFound = errors.New("archive store: not found")
	ErrDisabled = errors.New("archive store is disabled")
)

func New() ArchiveStore {
	return new(archiveStore)
}

type ArchiveStore interface {
	app.Component
	Get(ctx context.Context, name string) (data io.ReadCloser, err error)
	Put(ctx context.Context, name string, data io.ReadSeeker) (err error)
	Delete(ctx context.Context, name string) (err error)
}

type archiveStore struct {
	sess      *session.Session
	bucket    *string
	client    *s3.S3
	keyPrefix string
	enabled   bool
}

func (as *archiveStore) Init(a *app.App) (err error) {
	conf := a.MustComponent("config").(configSource).GetS3Store()
	if !conf.Enabled {
		return
	}
	if conf.Profile == "" {
		conf.Profile = "default"
	}
	if conf.Bucket == "" {
		return fmt.Errorf("s3 bucket is empty")
	}

	var endpoint *string
	if conf.Endpoint != "" {
		endpoint = aws.String(conf.Endpoint)
	}

	var creds *credentials.Credentials
	// If creds are provided in the configuration, they are directly forwarded to the client as static credentials.
	// This is mainly used for self-hosted scenarii where users store the data in a S3-compatible object store. In that
	// case it does not really make sense to create an AWS configuration since there is no related AWS account.
	// If credentials are not provided in the config however, the AWS credentials are determined by the SDK.
	if conf.Credentials.AccessKey != "" && conf.Credentials.SecretKey != "" {
		creds = credentials.NewStaticCredentials(conf.Credentials.AccessKey, conf.Credentials.SecretKey, "")
	}

	as.sess, err = session.NewSessionWithOptions(session.Options{
		Profile: conf.Profile,
		Config: aws.Config{
			Region:      aws.String(conf.Region),
			Endpoint:    endpoint,
			Credentials: creds,
			// By default S3 client uses virtual hosted bucket addressing when possible but this cannot work
			// for self-hosted. We can switch to path style instead with a configuration flag.
			S3ForcePathStyle: aws.Bool(conf.ForcePathStyle),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create session to s3: %v", err)
	}
	as.bucket = aws.String(conf.Bucket)

	as.client = s3.New(as.sess)
	as.keyPrefix = conf.KeyPrefix + "/"
	as.enabled = true
	return
}

func (as *archiveStore) Name() (name string) {
	return CName
}

func (as *archiveStore) Get(ctx context.Context, name string) (data io.ReadCloser, err error) {
	if !as.enabled {
		return nil, ErrDisabled
	}
	name = as.keyPrefix + name
	obj, err := as.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: as.bucket,
		Key:    aws.String(name),
	})
	if err != nil {
		if strings.HasPrefix(err.Error(), s3.ErrCodeNoSuchKey) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return obj.Body, nil
}

func (as *archiveStore) Put(ctx context.Context, name string, data io.ReadSeeker) (err error) {
	if !as.enabled {
		return ErrDisabled
	}
	name = as.keyPrefix + name
	_, err = as.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Key:    aws.String(name),
		Body:   data,
		Bucket: as.bucket,
	})
	return
}

func (as *archiveStore) Delete(ctx context.Context, name string) (err error) {
	if !as.enabled {
		return ErrDisabled
	}
	name = as.keyPrefix + name
	_, err = as.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: as.bucket,
		Key:    aws.String(name),
	})
	return
}
