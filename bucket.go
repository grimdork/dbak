package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Bucket in S3 or compatible system.
type Bucket struct {
	srv      *s3.S3
	Name     string
	contents []*s3.Object
}

// NewBucket sets up a new bucket for access.
func NewBucket(srv *s3.S3, name string) *Bucket {
	b := &Bucket{
		srv:  srv,
		Name: name,
	}
	return b
}

// List contents of bucket.
func (b *Bucket) List() ([]*s3.Object, error) {
	// b.srv.ListObjects()
	res, err := b.srv.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(b.Name)})
	if err != nil {
		return nil, err
	}

	b.contents = res.Contents
	return b.contents, nil
}
