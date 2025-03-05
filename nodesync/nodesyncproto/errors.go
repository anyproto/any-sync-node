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
)
