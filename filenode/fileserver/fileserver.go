package fileserver

import (
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/commonfile/fileblockstore"
	"github.com/anytypeio/any-sync/commonfile/fileproto"
	"github.com/anytypeio/any-sync/net/rpc/server"
)

const CName = "common.commonfile.fileservice"

func New() FileServer {
	return &fileServer{}
}

type FileServer interface {
	app.Component
}

type fileServer struct {
	store fileblockstore.BlockStore
}

func (f *fileServer) Init(a *app.App) (err error) {
	f.store = a.MustComponent(fileblockstore.CName).(fileblockstore.BlockStore)
	return fileproto.DRPCRegisterFile(a.MustComponent(server.CName).(server.DRPCServer), &rpcHandler{store: f.store})
}

func (f *fileServer) Name() (name string) {
	return CName
}
