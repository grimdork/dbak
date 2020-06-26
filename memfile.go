package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Implements os.FileInfo, Reader and Writer interfaces.
// These are the 3 interfaces necessary for the Handlers.
type memFile struct {
	sync.RWMutex
	name    string
	modtime time.Time
	symlink string
	content []byte
	pos     int64
	isdir   bool
}

// factory to make sure modtime is set
func newMemFile(name string, isdir bool) *memFile {
	return &memFile{
		name:    name,
		modtime: time.Now(),
		isdir:   isdir,
	}
}

// Have memFile fulfill os.FileInfo interface
func (f *memFile) Name() string { return filepath.Base(f.name) }
func (f *memFile) Size() int64  { return int64(len(f.content)) }
func (f *memFile) Mode() os.FileMode {
	ret := os.FileMode(0644)
	if f.isdir {
		ret = os.FileMode(0755) | os.ModeDir
	}
	if f.symlink != "" {
		ret = os.FileMode(0777) | os.ModeSymlink
	}
	return ret
}

// ModTime retuns the timestamp of the last modification.
func (f *memFile) ModTime() time.Time { return f.modtime }

// IsDir returns true if this is a directory.
func (f *memFile) IsDir() bool { return f.isdir }

// Sys returns size and ownership info.
func (f *memFile) Sys() interface{} {
	return &syscall.Stat_t{
		Uid:  65534,
		Gid:  65534,
		Size: int64(len(f.content)),
	}
}

// ReaderAt implementaton.
func (f *memFile) ReaderAt() (io.ReaderAt, error) {
	if f.isdir {
		return nil, os.ErrInvalid
	}
	return bytes.NewReader(f.content), nil
}

// WriterAt implementation.
func (f *memFile) WriterAt() (io.WriterAt, error) {
	if f.isdir {
		return nil, os.ErrInvalid
	}

	return f, nil
}

// WriteAt implements the actual filling of the memory buffer.
func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.Lock()
	defer f.Unlock()
	plen := len(p) + int(off)
	if plen >= len(f.content) {
		nc := make([]byte, plen)
		copy(nc, f.content)
		f.content = nc
	}
	c := copy(f.content[off:], p)
	var err error
	f.pos += int64(c)
	if f.pos >= int64(len(f.content)) {
		err = io.EOF
	}

	f.modtime = time.Now()
	return c, err
}

// Read copies from the beginning of the internal buffer to a supplied buffer.
func (f *memFile) Read(p []byte) (int, error) {
	pr("Read(): %d byte buffer", len(p))
	c, err := f.ReadAt(p, f.pos)
	pr("Read(): %d,%v", c, err)
	return c, err
}

// ReadAt copies out from the file buffer at an offset to a supplied buffer.
func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.Lock()
	defer f.Unlock()
	if f.pos > int64(len(f.content)) {
		f.pos = int64(len(f.content))
	}
	if f.pos == int64(len(f.content)) {
		return 0, io.EOF
	}

	pr("Copy %d bytes at offset %d to %p", len(f.content[off:]), off, p)
	c := copy(p, f.content[off:])
	return c, nil
}

// Reset position.
func (f *memFile) Reset() {
	f.pos = 0
}
