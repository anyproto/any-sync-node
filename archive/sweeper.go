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
	// sweepMinDiskAge disables collection of objects without an index entry
	// until the storage root is old enough: after a disk replacement the index
	// starts empty while the node's prefix still holds all its archived spaces,
	// and those objects may be the node's only copies until anti-entropy
	// rebuilds the index.
	sweepMinDiskAge = time.Hour * 24 * 30
)

// sweep garbage-collects this node's archive prefix: objects that don't back
// an Archived index entry (leftovers of restores, finished handoffs, deleted
// spaces) are removed once they are old enough. It shares archiveMu with the
// periodic archiver so an object can't be re-archived while being judged.
func (a *archive) sweep(ctx context.Context) (err error) {
	index := a.storageProvider.IndexStorage()
	diskAge := time.Since(a.storageProvider.DiskGen().CreatedTime)
	var checked, deleted, kept int
	err = a.archiveStore.List(ctx, func(name string, lastModified time.Time) (bool, error) {
		checked++
		if time.Since(lastModified) <= sweepMinObjectAge {
			return true, nil
		}
		// the archiver can't flip the status or overwrite the object while we
		// hold the lock, so the decision and the delete can't race a re-archive
		a.archiveMu.Lock()
		remove, rErr := a.sweepDecision(ctx, index, name, diskAge)
		if rErr != nil {
			a.archiveMu.Unlock()
			return false, rErr
		}
		if !remove {
			a.archiveMu.Unlock()
			kept++
			return true, nil
		}
		dErr := a.archiveStore.Delete(ctx, name)
		a.archiveMu.Unlock()
		if dErr != nil {
			log.Warn("archive sweep: can't delete object", zap.String("name", name), zap.Error(dErr))
		} else {
			deleted++
			a.stat.swept.Add(1)
		}
		return true, nil
	})
	if err != nil {
		return
	}
	if deleted > 0 || kept > 0 {
		log.Info("archive sweep finished", zap.Int("objects", checked), zap.Int("deleted", deleted), zap.Int("keptStale", kept))
	}
	return
}

func (a *archive) sweepDecision(ctx context.Context, index nodestorage.IndexStorage, name string, diskAge time.Duration) (remove bool, err error) {
	entry, entryErr := index.SpaceStatusEntry(ctx, name)
	switch {
	case errors.Is(entryErr, anystore.ErrDocNotFound):
		// unknown space: a leak from an aborted adoption — but after a disk
		// replacement the empty index makes every object look unknown while
		// the objects may be the only copies; wait until the disk is old
		// enough for anti-entropy to have rebuilt the index
		return diskAge > sweepMinDiskAge, nil
	case entryErr != nil:
		return false, entryErr
	}
	switch entry.Status {
	case nodestorage.SpaceStatusRemove, nodestorage.SpaceStatusMoved:
		// deleted or handed off: the object is garbage
		return true, nil
	case nodestorage.SpaceStatusOk:
		// restore/handoff leftover — but only when the live db actually exists
		// locally; a status of Ok without a db (e.g. a cancelled deletion of an
		// archived space) means the object may be our only copy
		return a.storageProvider.SpaceExists(name), nil
	default:
		// Archived backs the space; RemovePrepare may be cancelled;
		// Error/NotResponsible are left for the operator
		return false, nil
	}
}
