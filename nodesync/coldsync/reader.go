package coldsync

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/anytypeio/any-sync-node/nodesync/nodesyncproto"
	"go.uber.org/multierr"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

type streamReader struct {
	dir          string
	stream       nodesyncproto.DRPCNodeSync_ColdSyncClient
	needFlush    bool
	lastFilename string
	lastFile     *os.File
	lastGzip     *gzip.Reader
	lastReader   *bufio.ReadWriter
	pr           *io.PipeReader
	pw           *io.PipeWriter
	copyCh       chan error
}

func (sr *streamReader) Read(ctx context.Context) (err error) {
	defer func() {
		if err == io.EOF {
			err = sr.flush(ctx)
		}
	}()
	for {
		var msg *nodesyncproto.ColdSyncResponse
		msg, err = sr.stream.Recv()
		if err != nil {
			return
		}
		if err = sr.writeChunk(ctx, msg); err != nil {
			return
		}
	}
}

func (sr *streamReader) writeChunk(ctx context.Context, msg *nodesyncproto.ColdSyncResponse) (err error) {
	if sr.lastFilename != msg.Filename {
		if err = sr.flush(ctx); err != nil {
			return
		}
		sr.lastFilename = msg.Filename
		if sr.lastFile, err = os.Create(filepath.Join(sr.dir, sr.lastFilename)); err != nil {
			return
		}
		sr.pr, sr.pw = io.Pipe()
		if sr.lastGzip, err = gzip.NewReader(sr.pr); err != nil {
			return
		}
		go func() {
			_, e := io.Copy(sr.lastFile, sr.lastGzip)
			sr.copyCh <- e
		}()
	}
	if crc := crc32.ChecksumIEEE(msg.Data); crc != msg.Crc32 {
		return fmt.Errorf("crc32 mismatched")
	}
	_, err = io.Copy(sr.pw, bytes.NewReader(msg.Data))
	if err != nil {
		return err
	}
	return
}

func (sr *streamReader) flush(ctx context.Context) (err error) {
	var errs []error
	if sr.needFlush {
		select {
		case err = <-sr.copyCh:
			errs = append(errs, err)
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
		}
	}
	if sr.pw != nil {
		if err = sr.pw.Close(); err != nil {
			errs = append(errs, err)
		}
		sr.pw = nil
		if err = sr.pr.Close(); err != nil {
			errs = append(errs, err)
		}
		sr.pr = nil
	}
	if sr.lastGzip != nil {
		if err = sr.lastGzip.Close(); err != nil {
			errs = append(errs, err)
		}
		sr.lastGzip = nil
	}
	if sr.lastFile != nil {
		if err = sr.lastFile.Close(); err != nil {
			errs = append(errs, err)
		}
		sr.lastFile = nil
	}
	sr.needFlush = false
	return multierr.Combine(errs...)
}
