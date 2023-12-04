package nodesync

import (
	"context"

	"github.com/anyproto/any-sync/app/ldiff"
	"golang.org/x/exp/slices"

	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

type nodeRemoteDiff struct {
	partId int
	cl     nodesyncproto.DRPCNodeSyncClient
}

func (n nodeRemoteDiff) Ranges(ctx context.Context, ranges []ldiff.Range, resBuf []ldiff.RangeResult) (results []ldiff.RangeResult, err error) {
	protoRanges := make([]*nodesyncproto.PartitionSyncRange, len(ranges))
	for i, r := range ranges {
		protoRanges[i] = &nodesyncproto.PartitionSyncRange{
			From:     r.From,
			To:       r.To,
			Elements: r.Elements,
		}
	}
	req := &nodesyncproto.PartitionSyncRequest{
		PartitionId: uint64(n.partId),
		Ranges:      protoRanges,
	}
	resp, err := n.cl.PartitionSync(ctx, req)
	if err != nil {
		return nil, err
	}

	results = slices.Grow(resBuf, len(resp.Results))[0:len(resp.Results)]
	for i, res := range resp.Results {
		var elements []ldiff.Element
		if len(res.Elements) > 0 {
			elements = make([]ldiff.Element, len(res.Elements))
			for j, el := range res.Elements {
				elements[j] = ldiff.Element{
					Id:   el.Id,
					Head: el.Head,
				}
			}
		}
		results[i] = ldiff.RangeResult{
			Hash:     res.Hash,
			Elements: elements,
			Count:    int(res.Count),
		}
	}
	return
}

type nodeRemoteDiffHandler struct {
	nodehead nodehead.NodeHead
}

func (n *nodeRemoteDiffHandler) PartitionSync(ctx context.Context, req *nodesyncproto.PartitionSyncRequest) (*nodesyncproto.PartitionSyncResponse, error) {
	ld := n.nodehead.LDiff(int(req.PartitionId))
	var ranges = make([]ldiff.Range, len(req.Ranges))
	for i, r := range req.Ranges {
		ranges[i] = ldiff.Range{
			From:     r.From,
			To:       r.To,
			Elements: r.Elements,
		}
	}

	res, err := ld.Ranges(ctx, ranges, nil)
	if err != nil {
		return nil, err
	}

	protoResults := make([]*nodesyncproto.PartitionSyncResult, len(res))
	for i, r := range res {
		var elements []*nodesyncproto.PartitionSyncResultElement
		if len(r.Elements) > 0 {
			elements = make([]*nodesyncproto.PartitionSyncResultElement, len(r.Elements))
			for j, el := range r.Elements {
				elements[j] = &nodesyncproto.PartitionSyncResultElement{
					Id:   el.Id,
					Head: el.Head,
				}
			}
		}
		protoResults[i] = &nodesyncproto.PartitionSyncResult{
			Hash:     r.Hash,
			Elements: elements,
			Count:    uint32(r.Count),
		}
	}
	return &nodesyncproto.PartitionSyncResponse{
		Results: protoResults,
	}, nil
}
