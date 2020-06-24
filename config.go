package main

import (
	"encoding/json"
	"io/ioutil"
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
