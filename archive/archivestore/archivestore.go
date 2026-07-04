//go:generate mockgen -destination mock_archivestore/mock_archivestore.go github.com/anyproto/any-sync-node/archive/archivestore ArchiveStore
package archivestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	// Exists checks the object presence in this node's prefix without fetching it.
	Exists(ctx context.Context, name string) (ok bool, err error)
	// Key returns the full bucket key for the given name in this node's prefix.
	Key(name string) string
	// CopyFrom server-side copies an object from an absolute bucket key
	// (typically another node's prefix in the shared bucket) into this node's prefix.
	CopyFrom(ctx context.Context, srcKey, name string) (err error)
	// Shared reports whether the bucket is shared across the network's tree nodes,
	// enabling S3-mediated space migration.
	Shared() bool
	// List iterates all objects in this node's prefix; iter receives the object
	// name (key without the prefix) and its last-modified time, returning false
	// to stop the iteration.
	List(ctx context.Context, iter func(name string, lastModified time.Time) (bool, error)) (err error)
}

type archiveStore struct {
	sess      *session.Session
	bucket    *string
	client    *s3.S3
	keyPrefix string
	enabled   bool
	shared    bool
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
	as.shared = conf.Shared
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

// Exists deliberately uses ListObjectsV2 instead of HeadObject: GCS's
// S3-interop layer can serve stale HEAD responses for several seconds after a
// mutation (observed: HEAD returns 200 for a just-deleted object when the same
// key was HEAD'ed before), while listing is read-after-write consistent.
// Exists backs the durable-ACK check of the resharding handoff, where a stale
// positive could acknowledge an object that is already gone.
func (as *archiveStore) Exists(ctx context.Context, name string) (ok bool, err error) {
	if !as.enabled {
		return false, ErrDisabled
	}
	key := as.keyPrefix + name
	out, err := as.client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:  as.bucket,
		Prefix:  aws.String(key),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return false, err
	}
	for _, obj := range out.Contents {
		if obj.Key != nil && *obj.Key == key {
			return true, nil
		}
	}
	return false, nil
}

func (as *archiveStore) Key(name string) string {
	return as.keyPrefix + name
}

func (as *archiveStore) CopyFrom(ctx context.Context, srcKey, name string) (err error) {
	if !as.enabled {
		return ErrDisabled
	}
	_, err = as.client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:     as.bucket,
		CopySource: aws.String(url.PathEscape(*as.bucket + "/" + srcKey)),
		Key:        aws.String(as.keyPrefix + name),
	})
	if err != nil && isNotFoundErr(err) {
		return ErrNotFound
	}
	return
}

func (as *archiveStore) Shared() bool {
	return as.enabled && as.shared
}

func (as *archiveStore) List(ctx context.Context, iter func(name string, lastModified time.Time) (bool, error)) (err error) {
	if !as.enabled {
		return ErrDisabled
	}
	var iterErr error
	err = as.client.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: as.bucket,
		Prefix: aws.String(as.keyPrefix),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			name := strings.TrimPrefix(*obj.Key, as.keyPrefix)
			var lastModified time.Time
			if obj.LastModified != nil {
				lastModified = *obj.LastModified
			}
			cont, iErr := iter(name, lastModified)
			if iErr != nil {
				iterErr = iErr
				return false
			}
			if !cont {
				return false
			}
		}
		return !lastPage
	})
	if err == nil {
		err = iterErr
	}
	return
}

func isNotFoundErr(err error) bool {
	var awsErr awserr.RequestFailure
	if errors.As(err, &awsErr) {
		return awsErr.StatusCode() == http.StatusNotFound || awsErr.Code() == s3.ErrCodeNoSuchKey
	}
	return strings.HasPrefix(err.Error(), s3.ErrCodeNoSuchKey) || strings.HasPrefix(err.Error(), "NotFound")
}
