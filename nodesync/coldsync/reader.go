package coldsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/multierr"

	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

var ErrIncorrectFile = errors.New("incorrect file")

type streamReader struct {
	dir    string
	stream nodesyncproto.DRPCNodeSync_ColdSyncClient
	saver  *fileSaver
}

func (sr *streamReader) Read(ctx context.Context) (err error) {
	defer func() {
		if err == io.EOF && sr.saver != nil {
			err = sr.saver.Close(ctx)
		}
	}()
	for {
		var msg *nodesyncproto.ColdSyncResponse
		msg, err = sr.stream.Recv()
		if err != nil {
			return
		}
		if !strings.Contains(msg.Filename, "store") {
			return ErrIncorrectFile
		}
		if err = sr.writeChunk(ctx, msg); err != nil {
			return
		}
	}
}

func (sr *streamReader) writeChunk(ctx context.Context, msg *nodesyncproto.ColdSyncResponse) (err error) {
	if sr.saver == nil {
		if sr.saver, err = sr.newFileSaver(msg.Filename); err != nil {
			return
		}
	} else if sr.saver.name != msg.Filename {
		if err = sr.saver.Close(ctx); err != nil {
			return
		}
		if sr.saver, err = sr.newFileSaver(msg.Filename); err != nil {
			return
		}
	}
	return sr.saver.AddChunk(msg)
}

func (sr *streamReader) newFileSaver(name string) (fs *fileSaver, err error) {
	fs = &fileSaver{
		name: name,
		sr:   sr,
	}
	return fs, fs.init()
}

type fileSaver struct {
	name string
	sr   *streamReader
	f    *os.File
	pr   *io.PipeReader
	pw   *io.PipeWriter

	copierDone chan error
}

func (fs *fileSaver) init() (err error) {
	fpath := filepath.Join(fs.sr.dir, fs.name)
	if mkdirErr := os.MkdirAll(filepath.Dir(fpath), 0755); mkdirErr != nil {
		if !os.IsExist(mkdirErr) {
			return mkdirErr
		}
	}
	if fs.f, err = os.Create(fpath); err != nil {
		return err
	}
	fs.pr, fs.pw = io.Pipe()
	fs.copierDone = make(chan error, 1)
	go fs.copier()
	return
}

func (fs *fileSaver) AddChunk(msg *nodesyncproto.ColdSyncResponse) (err error) {
	if crc := crc32.ChecksumIEEE(msg.Data); crc != msg.Crc32 {
		return fmt.Errorf("crc32 mismatched")
	}
	if _, err = io.Copy(fs.pw, bytes.NewReader(msg.Data)); err != nil {
		return
	}
	return
}

func (fs *fileSaver) copier() {
	gr, err := gzip.NewReader(fs.pr)
	if err != nil {
		fs.copierDone <- err
		return
	}
	_, err = io.Copy(fs.f, gr)
	fs.copierDone <- err
}

func (fs *fileSaver) Close(ctx context.Context) error {
	var errs []error

	if err := fs.pw.Close(); err != nil {
		errs = append(errs, err)
	}

	select {
	case err := <-fs.copierDone:
		errs = append(errs, err)
	case <-ctx.Done():
		errs = append(errs, ctx.Err())
	}

	if err := fs.f.Close(); err != nil {
		errs = append(errs, err)
	}
	return multierr.Combine(errs...)
}
