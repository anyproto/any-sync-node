package statusservice

import (
	"context"
	"fmt"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/app/logger"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/commonspace/storage"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/nodeconf"
	treestorage "github.com/anytypeio/go-anytype-infrastructure-experiments/common/pkg/acl/storage"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/util/periodicsync"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/util/slice"
	"golang.org/x/exp/slices"
	"sync"
	"time"
)

const (
	statusServiceUpdateInterval = 5
	statusServiceTimeout        = time.Second
)

var log = logger.NewNamed("commonspace.statusservice")

type UpdateReceiver interface {
	UpdateTree(ctx context.Context, treeId string, status SyncStatus) (err error)
	UpdateNodeConnection(online bool)
}

type StatusService interface {
	HeadsChange(treeId string, heads []string)
	HeadsReceive(senderId, treeId string, heads []string)
	Watch(treeId string) (err error)
	Unwatch(treeId string)
	SetNodesOnline(senderId string, online bool)
	StateCounter() uint64
	RemoveAllExcept(senderId string, differentRemoteIds []string, stateCounter uint64)

	SetUpdateReceiver(updater UpdateReceiver)
	Run()
	Close() error
}

type SyncStatus int

const (
	SyncStatusUnknown SyncStatus = iota
	SyncStatusSynced
	SyncStatusNotSynced
)

type treeHeadsEntry struct {
	heads        []string
	stateCounter uint64
	syncStatus   SyncStatus
}

type treeStatus struct {
	treeId string
	status SyncStatus
	heads  []string
}

type statusService struct {
	sync.Mutex
	configuration  nodeconf.Configuration
	periodicSync   periodicsync.PeriodicSync
	updateReceiver UpdateReceiver
	storage        storage.SpaceStorage

	spaceId      string
	treeHeads    map[string]treeHeadsEntry
	watchers     map[string]struct{}
	stateCounter uint64
	nodesOnline  bool

	treeStatusBuf []treeStatus
}

func NewStatusService(spaceId string, configuration nodeconf.Configuration, store storage.SpaceStorage) StatusService {
	return &statusService{
		spaceId:       spaceId,
		treeHeads:     map[string]treeHeadsEntry{},
		watchers:      map[string]struct{}{},
		configuration: configuration,
		storage:       store,
		stateCounter:  0,
	}
}

func (s *statusService) SetUpdateReceiver(updater UpdateReceiver) {
	s.Lock()
	defer s.Unlock()

	s.updateReceiver = updater
}

func (s *statusService) Run() {
	s.periodicSync = periodicsync.NewPeriodicSync(
		statusServiceUpdateInterval,
		statusServiceTimeout,
		s.update,
		log)
	s.periodicSync.Run()
}

func (s *statusService) HeadsChange(treeId string, heads []string) {
	s.Lock()
	defer s.Unlock()

	var headsCopy []string
	headsCopy = append(headsCopy, heads...)

	s.treeHeads[treeId] = treeHeadsEntry{
		heads:        headsCopy,
		stateCounter: s.stateCounter,
		syncStatus:   SyncStatusNotSynced,
	}
	s.stateCounter++
}

func (s *statusService) SetNodesOnline(senderId string, online bool) {
	if !s.isSenderResponsible(senderId) {
		return
	}

	s.Lock()
	defer s.Unlock()

	s.nodesOnline = online
}

func (s *statusService) update(ctx context.Context) (err error) {
	s.treeStatusBuf = s.treeStatusBuf[:0]

	s.Lock()
	if s.updateReceiver == nil {
		s.Unlock()
		return
	}
	for treeId := range s.watchers {
		// that means that we haven't yet got the status update
		treeHeads, exists := s.treeHeads[treeId]
		if !exists {
			err = fmt.Errorf("treeHeads should always exist for watchers")
			s.Unlock()
			return
		}
		s.treeStatusBuf = append(s.treeStatusBuf, treeStatus{treeId, treeHeads.syncStatus, treeHeads.heads})
	}
	s.Unlock()
	s.updateReceiver.UpdateNodeConnection(s.nodesOnline)
	for _, entry := range s.treeStatusBuf {
		err = s.updateReceiver.UpdateTree(ctx, entry.treeId, entry.status)
		if err != nil {
			return
		}
	}
	return
}

func (s *statusService) HeadsReceive(senderId, treeId string, heads []string) {
	s.Lock()
	defer s.Unlock()

	curTreeHeads, ok := s.treeHeads[treeId]
	if !ok || curTreeHeads.syncStatus == SyncStatusSynced {
		return
	}

	// checking if other node is responsible
	if len(heads) == 0 || !s.isSenderResponsible(senderId) {
		return
	}

	// checking if we received the head that we are interested in
	for _, head := range heads {
		if idx, found := slices.BinarySearch(curTreeHeads.heads, head); found {
			curTreeHeads.heads[idx] = ""
		}
	}
	curTreeHeads.heads = slice.DiscardFromSlice(curTreeHeads.heads, func(h string) bool {
		return h == ""
	})
	if len(curTreeHeads.heads) == 0 {
		curTreeHeads.syncStatus = SyncStatusSynced
	}
	s.treeHeads[treeId] = curTreeHeads
}

func (s *statusService) Watch(treeId string) (err error) {
	s.Lock()
	defer s.Unlock()
	_, ok := s.treeHeads[treeId]
	if !ok {
		var (
			st    treestorage.TreeStorage
			heads []string
		)
		st, err = s.storage.TreeStorage(treeId)
		if err != nil {
			return
		}
		heads, err = st.Heads()
		if err != nil {
			return
		}
		slices.Sort(heads)
		s.stateCounter++
		s.treeHeads[treeId] = treeHeadsEntry{
			heads:        heads,
			stateCounter: s.stateCounter,
			syncStatus:   SyncStatusUnknown,
		}
	}

	s.watchers[treeId] = struct{}{}
	return
}

func (s *statusService) Unwatch(treeId string) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.watchers[treeId]; ok {
		delete(s.watchers, treeId)
	}
}

func (s *statusService) Close() (err error) {
	s.periodicSync.Close()
	return
}

func (s *statusService) StateCounter() uint64 {
	s.Lock()
	defer s.Unlock()

	return s.stateCounter
}

func (s *statusService) RemoveAllExcept(senderId string, differentRemoteIds []string, stateCounter uint64) {
	// if sender is not a responsible node, then this should have no effect
	if !s.isSenderResponsible(senderId) {
		return
	}

	s.Lock()
	defer s.Unlock()

	slices.Sort(differentRemoteIds)
	for treeId, entry := range s.treeHeads {
		// if the current update is outdated
		if entry.stateCounter > stateCounter {
			continue
		}
		// if we didn't find our treeId in heads ids which are different from us and node
		if _, found := slices.BinarySearch(differentRemoteIds, treeId); !found {
			entry.syncStatus = SyncStatusSynced
			s.treeHeads[treeId] = entry
		}
	}
}

func (s *statusService) isSenderResponsible(senderId string) bool {
	return slices.Contains(s.configuration.NodeIds(s.spaceId), senderId)
}