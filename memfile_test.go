package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestMemFile(t *testing.T) {
	f := NewMemFile("moo", false)
	buf := bytes.NewBufferString("Empty file.")
	f.WriteAt(buf.Bytes(), 0)
	f.Seek(0, io.SeekStart)
	tmp, err := ioutil.TempFile(os.TempDir(), "TestMemFile")
	if err != nil {
		t.Errorf("Error: %s", err.Error())
		t.FailNow()
	}

	t.Logf("Created %s", tmp.Name())
	defer tmp.Close()
	io.Copy(tmp, f)
	fi, err := tmp.Stat()
	if err != nil {
		t.Errorf("Couldn't Stat() output file: %s", err.Error())
		t.FailNow()
	}

	defer os.Remove(tmp.Name())
	if fi.Size() != f.Size() {
		t.Error("Output filesize doesn't match input.")
		t.FailNow()
	}

	t.Log("Input and output sizes match.")
}
