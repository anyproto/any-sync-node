package nodesyncproto

import (
	"errors"

	"github.com/anyproto/any-sync/net/rpc/rpcerr"
)

var (
	errGroup = rpcerr.ErrGroup(ErrCodes_ErrorOffset)

	ErrUnexpected             = errGroup.Register(errors.New("unexpected error"), uint64(ErrCodes_Unexpected))
	ErrExpectedCoordinator    = errGroup.Register(errors.New("this request should be sent by coordinator"), uint64(ErrCodes_ExpectedCoordinator))
	ErrUnsupportedStorageType = errGroup.Register(errors.New("unsupported storage"), uint64(ErrCodes_UnsupportedStorage))
	ErrSpaceDeleted           = errGroup.Register(errors.New("space is deleted"), uint64(ErrCodes_SpaceDeleted))
	ErrArchiveUnavailable     = errGroup.Register(errors.New("archive store is unavailable or not shared"), uint64(ErrCodes_ArchiveUnavailable))
	ErrArchiveObjectMissing   = errGroup.Register(errors.New("archive object is missing"), uint64(ErrCodes_ArchiveObjectMissing))
	ErrPeerIsNotNode          = errGroup.Register(errors.New("peer is not a network node"), uint64(ErrCodes_PeerIsNotNode))
	ErrNotResponsible         = errGroup.Register(errors.New("node is not responsible for the space"), uint64(ErrCodes_NotResponsible))
	ErrSpacePendingDeletion   = errGroup.Register(errors.New("space is pending deletion"), uint64(ErrCodes_SpacePendingDeletion))
)
