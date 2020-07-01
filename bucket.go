package main

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cheggaaa/pb/v3"
)

// Bucket in S3 or compatible system.
type Bucket struct {
	sess     *session.Session
	srv      *s3.S3
	Name     string
	contents []*s3.Object
}

// NewBucket sets up a new bucket for access.
func NewBucket(region, name string) (*Bucket, error) {
	awscfg := &aws.Config{
		Region: aws.String(region),
	}
	// Use shared credentials in ~/.aws/credentials or from envvars.
	// Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
	sess, err := session.NewSession(awscfg)
	if err != nil {
		return nil, err
	}

	_, err = sess.Config.Credentials.Get()
	if err != nil {
		return nil, err
	}

	b := &Bucket{
		sess: sess,
		srv:  s3.New(sess),
		Name: name,
	}

	_, err = b.List()
	return b, err
}

// List contents of bucket.
func (b *Bucket) List() ([]*s3.Object, error) {
	res, err := b.srv.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(b.Name)})
	if err != nil {
		return nil, err
	}

	b.contents = res.Contents
	return b.contents, nil
}

// Upload a file to S3, calling UploadGzip with a path argument.
func (b *Bucket) Upload(fn string) error {
	return b.UploadGzip("db", fn)
}

// UploadGzip compresses a file to S3 with progress.
// The filename will have .gz appended to it.
func (b *Bucket) UploadGzip(path, fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}

	defer f.Close()

	piper, pipew := io.Pipe()
	wg := sync.WaitGroup{}
	wg.Add(1)
	name := filepath.Join(path, filepath.Base(fn)+".gz")
	go func() {
		uploader := s3manager.NewUploader(b.sess)
		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(b.Name),
			Key:    aws.String(name),
			Body:   piper,
		})
		if err != nil {
			pr("Error uploading: %s", err.Error())
			time.Sleep(time.Millisecond * 500)
		}
		wg.Done()
	}()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	bar := pb.Full.Start64(fi.Size())
	reader := bar.NewProxyReader(f)

	gzw, _ := gzip.NewWriterLevel(pipew, gzip.BestCompression)
	_, err = io.Copy(gzw, reader)
	if err != nil {
		gzw.Close()
		return err
	}

	gzw.Close()
	pipew.Close()
	defer piper.Close()
	wg.Wait()
	bar.Finish()
	return nil
}

// Prune files older than a certain number of days.
func (b *Bucket) Prune(base string, days int) (int, error) {
	count := 0
	list, err := b.List()
	if err != nil {
		return 0, err
	}

	backups := []string{}
	for _, o := range list {
		s := *o.Key
		if s[0:3+len(base)] == "db/"+base {
			backups = append(backups, *o.Key)
		}
	}
	oldest := time.Now().AddDate(0, 0, -days).Format(base + "-20060102T150405.sql.gz")
	pr("Removing older than %s", oldest)
	for _, s := range backups {
		if filepath.Base(s) < oldest {
			pr("\tDeleting %s", filepath.Base(s))
			_, err = b.srv.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(b.Name), Key: aws.String(s)})
			if err != nil {
				return count, errors.New("Unable to delete " + s + ": " + err.Error())
			}

			err = b.srv.WaitUntilObjectNotExists(&s3.HeadObjectInput{
				Bucket: aws.String(b.Name),
				Key:    aws.String(s),
			})
			if err != nil {
				return count, errors.New(s + " did not get deleted: " + err.Error())
			}

			count++
		}
	}
	return count, nil
}
