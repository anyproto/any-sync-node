package oldstorage

import "github.com/anyproto/any-sync-node/nodestorage"

type configGetter interface {
	GetStorage() nodestorage.Config
}
