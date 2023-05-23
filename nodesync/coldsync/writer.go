package coldsync

import (
	"bufio"
	"compress/gzip"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const chunkSize = 1024 * 1024 // 1 Mb

type streamWriter struct {
	dir    string
	stream nodesyncproto.DRPCNodeSync_ColdSyncStream
	fw     *fileWriter
}

func (sw *streamWriter) Write() (err error) {
	if sw.dir, err = filepath.Abs(sw.dir); err != nil {
		return
	}
	return filepath.Walk(sw.dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		return sw.writeFile(path)
	})
}

func (sw *streamWriter) writeFile(path string) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer func() {
		_ = f.Close()
	}()
	filename := path[len(sw.dir):]
	fw := sw.newFileWriter(filename)
	bw := bufio.NewWriterSize(fw, chunkSize)
	gw := gzip.NewWriter(bw)
	if _, err = io.Copy(gw, f); err != nil {
		_ = gw.Close()
		return
	}
	if err = gw.Close(); err != nil {
		return
	}
	if err = bw.Flush(); err != nil {
		return
	}
	return
}

func (sw *streamWriter) newFileWriter(filename string) *fileWriter {
	if sw.fw == nil {
		sw.fw = &fileWriter{sw: sw}
	}
	sw.fw.filename = filename
	return sw.fw
}

type fileWriter struct {
	filename string
	sw       *streamWriter
}

func (f *fileWriter) Write(p []byte) (n int, err error) {
	if err = f.sw.stream.Send(&nodesyncproto.ColdSyncResponse{
		Filename: f.filename,
		Data:     p,
		Crc32:    crc32.ChecksumIEEE(p),
	}); err != nil {
		return
	}
	return len(p), nil
}
