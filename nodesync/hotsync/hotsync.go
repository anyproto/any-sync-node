package hotsync

import (
	"context"
	"github.com/anytypeio/any-sync-node/nodespace"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace"
	"github.com/anytypeio/any-sync/util/periodicsync"
	"golang.org/x/exp/slices"
	"sync"
	"time"
)

var log = logger.NewNamed(CName)

const CName = "node.nodesync.hotsync"

type HotSync interface {
	app.ComponentRunnable
	UpdateQueue(changedIds []string)
}

func New() HotSync {
	return new(hotSync)
}

type hotSync struct {
	spaceQueue   []string
	syncQueue    map[string]struct{}
	idle         time.Duration
	maxSynced    int
	spaceService nodespace.Service
	periodicSync periodicsync.PeriodicSync
	mx           sync.Mutex
}

func (h *hotSync) Init(a *app.App) (err error) {
	h.maxSynced = 300
	h.idle = time.Second * 20
	h.syncQueue = map[string]struct{}{}
	h.spaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	h.periodicSync = periodicsync.NewPeriodicSync(10, 0, h.checkCache, log)
	return
}

func (h *hotSync) Name() (name string) {
	return CName
}

func (h *hotSync) Run(ctx context.Context) (err error) {
	h.periodicSync.Run()
	return
}

func (h *hotSync) Close(ctx context.Context) (err error) {
	h.periodicSync.Close()
	return
}

func (h *hotSync) UpdateQueue(changedIds []string) {
	h.mx.Lock()
	defer h.mx.Unlock()
	slices.Sort(changedIds)
	for _, id := range h.spaceQueue {
		if idx, exists := slices.BinarySearch(changedIds, id); exists {
			changedIds[idx] = ""
		}
	}
	for _, id := range changedIds {
		if id != "" {
			h.spaceQueue = append(h.spaceQueue, id)
		}
	}
}

func (h *hotSync) checkCache(ctx context.Context) (err error) {
	removed := h.removeIdleSpaces(ctx)
	h.mx.Lock()
	newBatchLen := h.maxSynced - (len(h.syncQueue) - removed)
	cp := make([]string, 0, newBatchLen)
	copy(cp, h.spaceQueue[:newBatchLen])
	h.spaceQueue = h.spaceQueue[newBatchLen:]
	h.mx.Unlock()

	for _, id := range cp {
		_, err = h.spaceService.GetSpace(ctx, id)
		if err != nil {
			continue
		}
		h.syncQueue[id] = struct{}{}
	}
	return nil
}

func (h *hotSync) removeIdleSpaces(ctx context.Context) (removed int) {
	cache := h.spaceService.Cache()
	for id := range h.syncQueue {
		v, err := cache.Pick(ctx, id)
		if err != nil {
			continue
		}
		spc := v.(commonspace.Space)
		if time.Now().Sub(spc.LastUsage()) >= h.idle {
			_, _ = cache.Remove(ctx, id)
			removed++
			delete(h.syncQueue, id)
		}
	}
	return
}
