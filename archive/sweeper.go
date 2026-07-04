package archive

import (
	"context"
	"errors"
	"time"

	anystore "github.com/anyproto/any-store"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodestorage"
)

const (
	// sweepMinObjectAge protects fresh objects from the sweeper: an object may
	// legitimately exist for a live space for a while (restore keeps objects,
	// ForceArchive parks handoff snapshots), so only stale ones are collected.
	sweepMinObjectAge = time.Hour * 24 * 7
)

// sweep garbage-collects this node's archive prefix: objects that don't back
// an Archived index entry (leftovers of restores, finished handoffs, deleted
// spaces or a pre-replacement disk) are removed once they are old enough.
func (a *archive) sweep(ctx context.Context) (err error) {
	index := a.storageProvider.IndexStorage()
	var checked, deleted int
	err = a.archiveStore.List(ctx, func(name string, lastModified time.Time) (bool, error) {
		checked++
		entry, entryErr := index.SpaceStatusEntry(ctx, name)
		keep := true
		switch {
		case errors.Is(entryErr, anystore.ErrDocNotFound):
			// unknown space: a leak from a wiped disk or an aborted adoption
			keep = false
		case entryErr != nil:
			return false, entryErr
		default:
			switch entry.Status {
			case nodestorage.SpaceStatusArchived:
				keep = true
			case nodestorage.SpaceStatusRemove, nodestorage.SpaceStatusRemovePrepare, nodestorage.SpaceStatusMoved:
				keep = false
			case nodestorage.SpaceStatusOk:
				// restore/handoff leftover; age guard protects in-flight handoffs
				keep = false
			default:
				// Error, NotResponsible: leave for the operator
				keep = true
			}
		}
		if !keep && time.Since(lastModified) > sweepMinObjectAge {
			if dErr := a.archiveStore.Delete(ctx, name); dErr != nil {
				log.Warn("archive sweep: can't delete object", zap.String("name", name), zap.Error(dErr))
			} else {
				deleted++
				a.stat.swept.Add(1)
			}
		}
		return true, nil
	})
	if err != nil {
		return
	}
	if deleted > 0 {
		log.Info("archive sweep finished", zap.Int("objects", checked), zap.Int("deleted", deleted))
	}
	return
}
