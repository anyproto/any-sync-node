package nodestorage

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/cheggaaa/mb/v3"
)

type SpaceUpdate struct {
	SpaceId string
	OldHash string
	NewHash string
	Updated time.Time
}

type spaceUpdater struct {
	updateFunc func(update []SpaceUpdate)
	batcher    *mb.MB[SpaceUpdate]
	done       chan struct{}
}

func newSpaceUpdater(update func(updates []SpaceUpdate)) *spaceUpdater {
	return &spaceUpdater{
		batcher:    mb.New[SpaceUpdate](0),
		updateFunc: update,
		done:       make(chan struct{}),
	}
}

func (hu *spaceUpdater) Add(update SpaceUpdate) error {
	return hu.batcher.Add(context.Background(), update)
}

func (hu *spaceUpdater) Run() {
	go hu.process()
}

func (hu *spaceUpdater) process() {
	defer close(hu.done)
	for {
		msgs, err := hu.batcher.Wait(context.Background())
		if err != nil {
			return
		}
		msgs = removeDuplicatedUpdates(msgs)
		hu.updateFunc(msgs)
	}
}

func (hu *spaceUpdater) Close() error {
	_ = hu.batcher.Close()
	<-hu.done
	return nil
}

func removeDuplicatedUpdates(updates []SpaceUpdate) []SpaceUpdate {
	if len(updates) == 1 {
		return updates
	}
	slices.SortFunc(updates, func(a, b SpaceUpdate) int {
		cmp := strings.Compare(a.SpaceId, b.SpaceId)
		if cmp != 0 {
			return cmp
		}
		return a.Updated.Compare(b.Updated)
	})
	cnt := 0
	for i := 0; i < len(updates)-1; i++ {
		if updates[i].SpaceId != updates[i+1].SpaceId {
			updates[cnt] = updates[i]
			cnt++
		}
	}
	updates[cnt] = updates[len(updates)-1]
	cnt++
	return updates[:cnt]
}
