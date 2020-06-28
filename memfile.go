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
	buf     []byte
	pos     int64
	isdir   bool
}

// NewMemFile creates a new virtual file.
func NewMemFile(name string, isdir bool) *memFile {
	return &memFile{
		name:    name,
		modtime: time.Now(),
		isdir:   isdir,
	}
}

// Have memFile fulfill os.FileInfo interface
func (f *memFile) Name() string { return filepath.Base(f.name) }
func (f *memFile) Size() int64  { return int64(len(f.buf)) }
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
		Size: int64(len(f.buf)),
	}
}

// WriterAt implementation.
func (f *memFile) WriterAt() (io.WriterAt, error) {
	if f.isdir {
		return nil, os.ErrInvalid
	}

	return f, nil
}

// Write conforms to the io.Writer interface.
func (f *memFile) Write(p []byte) (int, error) {
	return f.WriteAt(p, f.pos)
}

// WriteAt implements the actual filling of the memory buffer.
func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.Lock()
	defer f.Unlock()
	plen := len(p) + int(off)
	if plen >= len(f.buf) {
		nc := make([]byte, plen)
		copy(nc, f.buf)
		f.buf = nc
	}
	c := copy(f.buf[off:], p)
	var err error
	f.pos += int64(c)
	if f.pos >= int64(len(f.buf)) {
		err = io.EOF
	}

	f.modtime = time.Now()
	return c, err
}

// ReaderAt implementaton.
func (f *memFile) ReaderAt() (io.ReaderAt, error) {
	if f.isdir {
		return nil, os.ErrInvalid
	}
	return bytes.NewReader(f.buf), nil
}

// Read copies from the beginning of the internal buffer to a supplied buffer.
func (f *memFile) Read(p []byte) (int, error) {
	c, err := f.ReadAt(p, f.pos)
	return c, err
}

// ReadAt copies out from the file buffer at an offset to a supplied buffer.
func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.Lock()
	defer f.Unlock()

	if f.pos >= f.Size() {
		return 0, io.EOF
	}

	c := copy(p, f.buf[off:])
	f.pos += int64(c)
	return c, nil
}

// Seek to a position in the file.
func (f *memFile) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		pos := f.pos + off
		if pos > f.Size() {
			return 0, io.ErrUnexpectedEOF
		}

		f.pos = pos
		return f.pos, nil

	case io.SeekStart:
		if off > f.Size() {
			return 0, io.ErrUnexpectedEOF
		}

		f.pos = off
		return f.pos, nil

	case io.SeekEnd:
		pos := f.Size() + off
		if pos > f.Size() {
			return 0, io.ErrUnexpectedEOF
		}
	}

	return 0, os.ErrInvalid
}

// WriteString conforms with io.StringWriter.
func (f *memFile) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}
