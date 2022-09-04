package acl

import (
	"context"
	"github.com/anytypeio/any-sync-consensusnode/consensusproto"
	"github.com/anytypeio/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anytypeio/any-sync/commonspace/objectsync/synchandler"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"go.uber.org/zap"
	"sync"
)

func newWatcher(spaceId, aclId string, h synchandler.SyncHandler) (w *watcher, err error) {
	w = &watcher{
		aclId:   aclId,
		spaceId: spaceId,
		handler: h,
		ready:   make(chan struct{}),
	}
	if w.logId, err = cidToByte(aclId); err != nil {
		return nil, err
	}
	return
}

type watcher struct {
	spaceId string
	aclId   string
	logId   []byte
	handler synchandler.SyncHandler
	ready   chan struct{}
	isReady sync.Once
	err     error
}

func (w *watcher) AddConsensusRecords(recs []*consensusproto.Record) {
	w.isReady.Do(func() {
		close(w.ready)
	})
	records := make([]*aclrecordproto.RawAclRecordWithId, 0, len(recs))

	for _, rec := range recs {
		recId, err := cidToString(rec.Id)
		if err != nil {
			log.Error("received invalid id from consensus node", zap.Error(err))
			continue
		}
		records = append(records, &aclrecordproto.RawAclRecordWithId{
			Payload: rec.Payload,
			Id:      recId,
		})
	}

	aclReq := &aclrecordproto.AclSyncMessage{
		Content: &aclrecordproto.AclSyncContentValue{
			Value: &aclrecordproto.AclSyncContentValue_AddRecords{
				AddRecords: &aclrecordproto.AclAddRecords{
					Records: records,
				},
			},
		},
	}
	payload, err := aclReq.Marshal()
	if err != nil {
		log.Error("acl payload marshal error", zap.Error(err))
		return
	}
	req := &spacesyncproto.ObjectSyncMessage{
		SpaceId:  w.spaceId,
		Payload:  payload,
		ObjectId: w.aclId,
	}

	if err = w.handler.HandleMessage(context.TODO(), "", req); err != nil {
		log.Warn("handle message error", zap.Error(err))
	}
}

func (w *watcher) AddConsensusError(err error) {
	w.isReady.Do(func() {
		w.err = err
		close(w.ready)
	})
}

func (w *watcher) Ready(ctx context.Context) (err error) {
	select {
	case <-w.ready:
		return w.err
	case <-ctx.Done():
		return ctx.Err()
	}
}