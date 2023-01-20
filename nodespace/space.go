package nodespace

import (
	"context"
	"github.com/anytypeio/any-sync/commonspace"
)

func newNodeSpace(cc commonspace.Space) (*nodeSpace, error) {
	return &nodeSpace{cc}, nil
}

type nodeSpace struct {
	commonspace.Space
}

func (s *nodeSpace) Init(ctx context.Context) (err error) {
	return s.Space.Init(ctx)
}

func (s *nodeSpace) Close() (err error) {
	return s.Space.Close()
}
