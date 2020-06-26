package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestMemFile(t *testing.T) {
	f := newMemFile("moo", false)
	buf := bytes.NewBufferString("Empty file.")
	f.WriteAt(buf.Bytes(), 0)
	f.Reset()
	f2, err := os.OpenFile("test", os.O_CREATE, 0644)
	if err != nil {
		t.Errorf("Error: %s", err.Error())
		t.FailNow()
	}
	defer f2.Close()
	io.Copy(f2, f)
}
