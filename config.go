package main

import (
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Config for database backups.
type Config struct {
	// Username for login.
	Username string `json:"username"`
	// Password of user, if any.
	Password string `json:"password"`
	// Host address to connect to.
	Host string `json:"host"`
	// Port to connect to.
	Port string `json:"port"`
	// Name of database to back up from.
	Name string `json:"name"`
	// Tables to back up.
	Tables []string `json:"tables"`
	// Region for S3 bucket.
	Region string `json:"region"`
	// Bucket to send dumps to.
	Bucket string `json:"bucket"`
}

func loadConfig(fn string) (*Config, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	err = json.Unmarshal(data, cfg)
	return cfg, err
}

// TbleDates holds the most recent update times for all tables.
type TableDates struct {
	Dates map[string]string `json:"dates"`
}

const tabledates = "tables.json"

// LoadTableDates returns a map of table names with last modified dates.
func (b *Bucket) LoadTableDates() *TableDates {
	td := &TableDates{Dates: make(map[string]string)}
	buf := newMemFile(tabledates, false)
	dl := s3manager.NewDownloader(b.sess)
	_, _ = dl.Download(buf,
		&s3.GetObjectInput{
			Bucket: aws.String(b.Name),
			Key:    aws.String(tabledates),
		})

	// Simply return an empty structure no matter what happened during download.
	json.Unmarshal(buf.content, &td)
	return td
}

func (b *Bucket) UpdateTableDates(td *TableDates) error {
	data, err := json.MarshalIndent(td, "", "\t")
	if err != nil {
		return err
	}

	f := newMemFile(tabledates, false)
	f.WriteAt(data, 0)
	f.Seek(0, io.SeekStart)

	r, w := io.Pipe()
	go func() {
		io.Copy(w, f)
		w.Close()
	}()
	uploader := s3manager.NewUploader(b.sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(b.Name),
		Key:    aws.String(tabledates),
		Body:   r,
	})
	r.Close()
	return err
}
