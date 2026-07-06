package repairer

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/coldsync"
)

const CName = "node.repairer"

var log = logger.NewNamed(CName)

const (
	defaultRepairIntervalMinutes = 60
	// spaceRepairTimeout bounds a single space repair (quarantine + cold pull)
	spaceRepairTimeout = time.Minute * 10
)

func New() Repairer {
	return new(repairer)
}

// Repairer periodically self-heals spaces whose index status is Error
// (failed archive, corrupted db, missing data) while this node is responsible
// for them. It first tries to repair in place (the error may have been
// transient); a genuinely broken db is quarantined under <root>/.quarantine
// (data preserved for the operator) and a valid copy is pulled from another
// responsible node with the regular coldsync — the same mechanic anti-entropy
// uses for missing spaces, so it works on every network, shared bucket or not.
type Repairer interface {
	app.ComponentRunnable
}

type configGetter interface {
	GetRepairer() Config
}

type Config struct {
	// RepairIntervalMinutes is the period of the repair cycle, default 60.
	RepairIntervalMinutes int `yaml:"repairIntervalMinutes"`
}

type repairer struct {
	nodeConf nodeconf.Service
	storage  nodestorage.NodeStorage
	coldSync coldsync.ColdSync
	conf     Config

	runCtx       context.Context
	runCtxCancel context.CancelFunc
	done         chan struct{}
	stat         *repairerStat

	// disableLoop keeps the background loop off (unit tests drive cycles directly)
	disableLoop bool
}

func (r *repairer) Init(a *app.App) (err error) {
	r.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	r.storage = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	r.coldSync = a.MustComponent(coldsync.CName).(coldsync.ColdSync)
	if g, ok := a.Component("config").(configGetter); ok {
		r.conf = g.GetRepairer()
	}
	if r.conf.RepairIntervalMinutes <= 0 {
		r.conf.RepairIntervalMinutes = defaultRepairIntervalMinutes
	}
	r.runCtx, r.runCtxCancel = context.WithCancel(context.Background())
	r.done = make(chan struct{})
	r.stat = new(repairerStat)
	if m := a.Component(metric.CName); m != nil {
		registerMetric(r.stat, m.(metric.Metric).Registry())
	}
	return
}

func (r *repairer) Name() (name string) {
	return CName
}

func (r *repairer) Run(_ context.Context) (err error) {
	if r.disableLoop {
		close(r.done)
		return
	}
	go r.loop()
	return
}

func (r *repairer) loop() {
	defer close(r.done)
	ticker := time.NewTicker(time.Duration(r.conf.RepairIntervalMinutes) * time.Minute)
	defer ticker.Stop()
	r.repairCycle()
	for {
		select {
		case <-r.runCtx.Done():
			return
		case <-ticker.C:
		}
		r.repairCycle()
	}
}

func (r *repairer) repairCycle() {
	var candidates []string
	err := r.storage.IndexStorage().ReadSpacesByStatus(r.runCtx, nodestorage.SpaceStatusError, func(spaceId string) (bool, error) {
		// spaces we are not responsible for are not repairable from owners
		// that way; they stay visible for the operator
		if r.nodeConf.IsResponsible(spaceId) {
			candidates = append(candidates, spaceId)
		}
		return true, nil
	})
	if err != nil {
		log.Warn("repair cycle: can't read index", zap.Error(err))
		return
	}
	r.stat.errored.Store(uint32(len(candidates)))
	if len(candidates) == 0 {
		return
	}
	log.Info("repair cycle started", zap.Int("spaces", len(candidates)))
	var repaired, failed int
	for _, spaceId := range candidates {
		if r.runCtx.Err() != nil {
			return
		}
		if err := r.repairSpace(spaceId); err != nil {
			failed++
			log.Warn("space repair failed", zap.String("spaceId", spaceId), zap.Error(err))
		} else {
			repaired++
		}
	}
	r.stat.errored.Store(uint32(failed))
	log.Info("repair cycle finished", zap.Int("repaired", repaired), zap.Int("failed", failed))
}

func (r *repairer) repairSpace(spaceId string) (err error) {
	ctx, cancel := context.WithTimeout(r.runCtx, spaceRepairTimeout)
	defer cancel()
	index := r.storage.IndexStorage()

	// the space must be openable for validation, and Error status blocks
	// opening; on any failure below the status flips back to Error
	if err = index.SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusOk, ""); err != nil {
		return
	}
	defer func() {
		if err != nil {
			if sErr := index.SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusError, ""); sErr != nil {
				log.Error("can't restore error status", zap.String("spaceId", spaceId), zap.Error(sErr))
			}
		}
	}()

	if r.storage.SpaceExists(spaceId) {
		// the error may have been transient (e.g. a failed archive upload):
		// if the db opens and indexes fine, the space is healthy in place
		if _, ipErr := r.storage.IndexSpace(ctx, spaceId, true); ipErr == nil {
			log.Info("space repaired in place", zap.String("spaceId", spaceId))
			r.stat.repairedInPlace.Add(1)
			return nil
		}
		// genuinely broken: park the data for the operator and pull a fresh copy
		quarantinePath, qErr := r.storage.QuarantineSpace(ctx, spaceId)
		if qErr != nil {
			return qErr
		}
		r.stat.quarantined.Add(1)
		log.Warn("corrupted space quarantined", zap.String("spaceId", spaceId), zap.String("path", quarantinePath))
	}

	// pull a valid copy from a responsible neighbor
	var pulled bool
	for _, peerId := range r.nodeConf.NodeIds(spaceId) {
		if pErr := r.coldSync.Sync(ctx, spaceId, peerId); pErr != nil {
			log.Debug("repair: cold pull failed", zap.String("spaceId", spaceId), zap.String("peerId", peerId), zap.Error(pErr))
			continue
		}
		pulled = true
		break
	}
	if !pulled {
		return errNoValidCopy
	}
	// validate and register the pulled copy (index hashes + nodehead)
	if _, err = r.storage.IndexSpace(ctx, spaceId, true); err != nil {
		return err
	}
	log.Info("space repaired from a neighbor", zap.String("spaceId", spaceId))
	r.stat.repaired.Add(1)
	return nil
}

func (r *repairer) Close(_ context.Context) (err error) {
	r.runCtxCancel()
	select {
	case <-r.done:
	case <-time.After(time.Second * 10):
	}
	return
}
