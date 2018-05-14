// Package renterutil provides convenience functions for common renter
// actions.
package renterutil

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// assume metafiles have this extension
const metafileExt = ".usa"

// ErrCanceled indicates that the Operation was canceled.
var ErrCanceled = errors.New("canceled")

// An Operation represents a long-running operation, such as an upload,
// download, or migration.
type Operation struct {
	updates chan interface{}
	cancel  chan struct{}
	err     error
}

func (op *Operation) die(err error) {
	op.err = err
	close(op.updates)
}

func (op *Operation) sendUpdate(u interface{}) {
	select {
	case op.updates <- u:
	default:
	}
}

// Updates returns op's update channel. Callers should use a type switch to
// distinguish between various types of update. The channel is closed when op
// completes.
func (op *Operation) Updates() <-chan interface{} {
	return op.updates
}

// Cancel cancels op, causing Err to report ErrCanceled.
func (op *Operation) Cancel() {
	close(op.cancel)
}

// Canceled returns whether op was canceled.
func (op *Operation) Canceled() bool {
	select {
	case <-op.cancel:
		return true
	default:
		return false
	}
}

// Err returns the error that caused op to fail. It is only valid after the
// Updates channel has been closed.
func (op *Operation) Err() error {
	return op.err
}

func newOperation() *Operation {
	op := &Operation{
		updates: make(chan interface{}),
		cancel:  make(chan struct{}),
	}
	return op
}

// A TransferProgressUpdate details the number of bytes transferred during the
// course of an Operation.
type TransferProgressUpdate struct {
	Total       int64
	Start       int64
	Transferred int64
}

// A DirQueueUpdate indicates that a file has been queued as part of a multi-
// file Operation.
type DirQueueUpdate struct {
	Filename string
	Filesize int64
}

// A DirSkipUpdate indicates that a file has been skipped as part of a multi-
// file Operation.
type DirSkipUpdate struct {
	Filename string
	Err      error
}

// FileIter is an iterator that returns the next filepath and the filepath of
// the file's metafile. It should return io.EOF to signal the end of
// iteration.
type FileIter func() (string, string, error)

// NewShallowFileIter returns a FileIter that iterates over a single
// directory.
func NewShallowFileIter(dir, metaDir string) FileIter {
	d, err := os.Open(dir)
	if err != nil {
		return func() (string, string, error) {
			return "", "", err
		}
	}
	names, err := d.Readdirnames(-1)
	if err != nil {
		return func() (string, string, error) {
			return "", "", err
		}
	}
	i := 0
	return func() (string, string, error) {
		if i >= len(names) {
			return "", "", io.EOF
		}
		filePath := filepath.Join(dir, names[i])
		metaPath := filepath.Join(metaDir, names[i]) + metafileExt
		i++
		return filePath, metaPath, nil
	}
}

// NewRecursiveFileIter returns a FileIter that iterates over a nested set of
// directories.
func NewRecursiveFileIter(dir, metaDir string) FileIter {
	type walkFile struct {
		name string
		err  error
	}
	fileChan := make(chan walkFile)
	go func() {
		filepath.Walk(dir, func(name string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				fileChan <- walkFile{strings.TrimPrefix(name, dir), err}
			}
			return nil
		})
		close(fileChan)
	}()
	return func() (string, string, error) {
		wf, ok := <-fileChan
		if !ok {
			return "", "", io.EOF
		} else if wf.err != nil {
			return "", "", wf.err
		}
		filePath := filepath.Join(dir, wf.name)
		metaPath := filepath.Join(metaDir, wf.name) + metafileExt
		return filePath, metaPath, nil
	}
}

// NewRecursiveMetaFileIter returns a FileIter that iterates over a nested set
// of directories, metafiles first.
func NewRecursiveMetaFileIter(metaDir, dir string) FileIter {
	type walkFile struct {
		name string
		err  error
	}
	fileChan := make(chan walkFile)
	go func() {
		filepath.Walk(metaDir, func(name string, info os.FileInfo, err error) error {
			if !info.IsDir() && filepath.Ext(name) == metafileExt {
				fileChan <- walkFile{strings.TrimPrefix(name, metaDir), err}
			}
			return nil
		})
		close(fileChan)
	}()
	return func() (string, string, error) {
		wf, ok := <-fileChan
		if !ok {
			return "", "", io.EOF
		} else if wf.err != nil {
			return "", "", wf.err
		}
		metaPath := filepath.Join(metaDir, wf.name)
		filePath := strings.TrimSuffix(filepath.Join(dir, wf.name), metafileExt)
		return metaPath, filePath, nil
	}
}