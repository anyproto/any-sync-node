package nodespace

import (
	"context"
	"github.com/anyproto/any-sync/commonspace"
)

func newNodeSpace(cc commonspace.Space) (*nodeSpace, error) {
	return &nodeSpace{cc}, nil
}

type nodeSpace struct {
	commonspace.Space
}

func (s *nodeSpace) Init(ctx context.Context) (err error) {
	err = s.Space.Init(ctx)
	if err != nil {
		return
	}
	s.Space.HeadSync().Run()
	return
}

func (s *nodeSpace) Close() (err error) {
	return s.Space.Close()
}
