package nodestorage

import (
	"cmp"
	"context"
	"fmt"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"golang.org/x/exp/slices"
)

func (r *nodeStorage) GetSpaceStats(ctx context.Context, treeTop int) (spaceStats ObjectSpaceStats, err error) {
	var (
		anyStore            = r.AnyStore()
		docsCount           = 0
		deletedObjectsCount = 0
		changesCount        = 0
		changesSize         = 0
		lengths             = make([]int, 0, 100)
	)
	if treeTop > 0 {
		spaceStats.treeMap = map[string]TreeStat{}
	}
	headsColl, err := anyStore.Collection(ctx, headstorage.HeadsCollectionName)
	if err != nil {
		err = fmt.Errorf("collection not found: %w", err)
		return
	}
	deletedObjectsCount, err = headsColl.Find(query.Key{Path: []string{headstorage.DeletedStatusKey}, Filter: query.NewComp(query.CompOpGte, int(headstorage.DeletedStatusQueued))}).Count(ctx)
	if err != nil {
		err = fmt.Errorf("count not found: %w", err)
		return
	}
	changesColl, err := anyStore.Collection(ctx, objecttree.CollName)
	if err != nil {
		err = fmt.Errorf("collection not found: %w", err)
		return
	}
	qry := changesColl.Find(query.All{}).Sort(objecttree.TreeKey)
	iter, err := qry.Iter(ctx)
	if err != nil {
		err = fmt.Errorf("iter not found: %w", err)
	}
	defer iter.Close()
	treeStat := TreeStat{Id: ""}
	for iter.Next() {
		var doc anystore.Doc
		doc, err = iter.Doc()
		if err != nil {
			err = fmt.Errorf("doc not found: %w", err)
			return
		}
		changesCount++
		newId := doc.Value().GetString(objecttree.TreeKey)
		if treeStat.Id != newId {
			if treeStat.Id != "" {
				if treeTop > 0 {
					spaceStats.treeMap[treeStat.Id] = treeStat
				}
				docsCount++
			}
			treeStat = TreeStat{Id: newId}
		}
		treeStat.ChangesCount++
		chSize := doc.Value().GetInt(objecttree.ChangeSizeKey)
		lengths = append(lengths, chSize)
		snapshotCounter := doc.Value().GetInt(objecttree.SnapshotCounterKey)
		treeStat.ChangesSumSize += chSize
		changesSize += chSize
		if snapshotCounter > treeStat.MaxSnapshotCounter {
			treeStat.MaxSnapshotCounter = snapshotCounter
		}
		if snapshotCounter != 0 {
			treeStat.SnapshotsCount++
		}
	}
	if treeStat.Id != "" {
		if treeTop > 0 {
			spaceStats.treeMap[treeStat.Id] = treeStat
		}
		docsCount++
	}
	slices.Sort(lengths)

	spaceStats.ObjectsCount = docsCount
	spaceStats.DeletedObjectsCount = deletedObjectsCount
	spaceStats.ChangesCount = changesCount
	spaceStats.ChangeSize.Median = calcMedian(lengths)
	spaceStats.ChangeSize.Avg = calcAvg(lengths)
	spaceStats.ChangeSize.P95 = calcP95(lengths)
	if len(lengths) > 0 {
		spaceStats.ChangeSize.MaxLen = lengths[len(lengths)-1]
	}
	spaceStats.ChangeSize.Total = changesSize

	if treeTop > 0 {
		for _, treeStat := range spaceStats.treeMap {
			spaceStats.TreeStats = append(spaceStats.TreeStats, treeStat)
		}
		slices.SortFunc(spaceStats.TreeStats, func(a, b TreeStat) int {
			res := cmp.Compare(b.ChangesCount, a.ChangesCount)
			if res == 0 {
				return cmp.Compare(b.ChangesSumSize, a.ChangesSumSize)
			} else {
				return res
			}
		})
		if len(spaceStats.TreeStats) > treeTop {
			spaceStats.TreeStats = spaceStats.TreeStats[:treeTop]
		}
	}
	return
}

func calcMedian(sortedLengths []int) (median float64) {
	mid := len(sortedLengths) / 2
	if len(sortedLengths)%2 == 0 {
		median = float64(sortedLengths[mid-1]+sortedLengths[mid]) / 2.0
	} else {
		median = float64(sortedLengths[mid])
	}
	return
}

func calcAvg(lengths []int) (avg float64) {
	sum := 0
	for _, n := range lengths {
		sum += n
	}

	avg = float64(sum) / float64(len(lengths))
	return
}

func calcP95(sortedLengths []int) (percentile float64) {
	if len(sortedLengths) == 1 {
		percentile = float64(sortedLengths[0])
		return
	}

	p := 95.0
	r := (p/100)*(float64(len(sortedLengths))-1.0) + 1
	ri := int(r)
	if r == float64(int64(r)) {
		percentile = float64(sortedLengths[ri-1])
	} else if r > 1 {
		rf := r - float64(ri)
		percentile = float64(sortedLengths[ri-1]) + rf*float64(sortedLengths[ri]-sortedLengths[ri-1])
	}

	return
}
