package spacechecker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/nodeconf"

	"github.com/anyproto/any-sync-node/nodestorage"
)

const CName = "node.debug.spacechecker"

var log = logger.NewNamed(CName)

type Result struct {
	SpaceId            string   `json:"spaceId"`
	LocalStatus        string   `json:"localStatus"`
	LocalStatusError   string   `json:"localStatusError,omitempty"`
	CoordinatorStatus  string   `json:"coordinatorStatus"`
	IsResponsible      bool     `json:"isResponsible"`
	SpaceStorageExists bool     `json:"spaceStorageExists"`
	IsFixed            bool     `json:"isFixed"`
	Problems           []string `json:"problems"`
	Log                []string `json:"log"`
}

type SpaceChecker interface {
	Check(ctx context.Context, spaceId string) (Result, error)
	Fix(ctx context.Context, spaceId string) (Result, error)
	app.Component
}

func New() SpaceChecker {
	return &spaceChecker{}
}

type spaceChecker struct {
	storageService nodestorage.NodeStorage
	coordClient    coordinatorclient.CoordinatorClient
	nodeConf       nodeconf.Service
}

func (s *spaceChecker) Init(a *app.App) (err error) {
	s.storageService = a.MustComponent(spacestorage.CName).(nodestorage.NodeStorage)
	s.coordClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	s.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	return nil
}

func (s *spaceChecker) Name() (name string) {
	return CName
}

func (s *spaceChecker) Check(ctx context.Context, spaceId string) (res Result, err error) {
	res.SpaceId = spaceId

	// 1. Get local status from index storage
	localStatusStr, localErr := s.getLocalStatus(ctx, spaceId, &res)
	if localErr != nil {
		return res, fmt.Errorf("get local status: %w", localErr)
	}

	// 2. Get coordinator status
	coordStatusStr, coordErr := s.getCoordinatorStatus(ctx, spaceId, &res)
	if coordErr != nil {
		return res, fmt.Errorf("get coordinator status: %w", coordErr)
	}

	// 3. Check if node is responsible
	res.IsResponsible = s.nodeConf.IsResponsible(spaceId)
	res.Log = append(res.Log, fmt.Sprintf("isResponsible: %v", res.IsResponsible))

	// 4. Check if storage exists
	storeDir := s.storageService.StoreDir(spaceId)
	_, statErr := os.Stat(storeDir)
	res.SpaceStorageExists = statErr == nil
	res.Log = append(res.Log, fmt.Sprintf("spaceStorageExists: %v (path: %s)", res.SpaceStorageExists, storeDir))

	// Validate state combinations
	s.validate(&res, localStatusStr, coordStatusStr)

	return res, nil
}

func (s *spaceChecker) Fix(ctx context.Context, spaceId string) (Result, error) {
	res, err := s.Check(ctx, spaceId)
	if err != nil {
		return res, err
	}
	if len(res.Problems) == 0 {
		return res, nil
	}

	coordStatus := res.CoordinatorStatus
	localStatus := res.LocalStatus
	storageExists := res.SpaceStorageExists
	isResponsible := res.IsResponsible
	indexStorage := s.storageService.IndexStorage()

	switch {
	// coordStatus: removed, localStatus: not removed or storageExists: true - remove space and switch local status
	case coordStatus == "removed" && (localStatus != "removed" || storageExists):
		if storageExists {
			err = s.storageService.DeleteSpaceStorage(ctx, spaceId)
			if err != nil && !errors.Is(err, spacestorage.ErrSpaceStorageMissing) {
				return res, fmt.Errorf("delete space storage: %w", err)
			}
			res.Log = append(res.Log, "fix: deleted space storage")
		}
		if localStatus != "removed" {
			err = indexStorage.SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusRemove, "")
			if err != nil {
				return res, fmt.Errorf("set status removed: %w", err)
			}
			res.Log = append(res.Log, "fix: set local status to removed")
		}
		res.IsFixed = true

	// coordStatus: remPrepare, localStatus: not remPrepare - switch local status
	case coordStatus == "remPrepare" && localStatus != "remPrepare":
		err = indexStorage.SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusRemovePrepare, "")
		if err != nil {
			return res, fmt.Errorf("set status remPrepare: %w", err)
		}
		res.Log = append(res.Log, "fix: set local status to remPrepare")
		res.IsFixed = true

	// coordStatus: ok, localStatus: ok, isResponsible: false - set notResponsible, move storage if exists
	case coordStatus == "ok" && localStatus == "ok" && !isResponsible:
		err = indexStorage.SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusNotResponsible, "")
		if err != nil {
			return res, fmt.Errorf("set status notResponsible: %w", err)
		}
		res.Log = append(res.Log, "fix: set local status to notResponsible")
		if storageExists {
			srcDir := s.storageService.StoreDir(spaceId)
			dstDir := s.storageService.StoreDir(filepath.Join("notresponsible", spaceId))
			if err = os.MkdirAll(filepath.Dir(dstDir), 0755); err != nil {
				return res, fmt.Errorf("create notresponsible dir: %w", err)
			}
			if err = os.Rename(srcDir, dstDir); err != nil {
				return res, fmt.Errorf("move storage: %w", err)
			}
			res.Log = append(res.Log, fmt.Sprintf("fix: moved storage from %s to %s", srcDir, dstDir))
		}
		res.IsFixed = true

	default:
		res.Log = append(res.Log, "fix: no automatic fix available for this state")
	}

	return res, nil
}

func (s *spaceChecker) getLocalStatus(ctx context.Context, spaceId string, res *Result) (statusStr string, err error) {
	status, err := s.storageService.IndexStorage().SpaceStatus(ctx, spaceId)
	if err != nil {
		res.LocalStatus = "unknown"
		res.LocalStatusError = err.Error()
		res.Log = append(res.Log, fmt.Sprintf("localStatus: error getting status: %s", err))
		return "unknown", err
	}
	statusStr = localStatusString(status)
	res.LocalStatus = statusStr
	res.Log = append(res.Log, fmt.Sprintf("localStatus: %s", statusStr))
	return statusStr, nil
}

func (s *spaceChecker) getCoordinatorStatus(ctx context.Context, spaceId string, res *Result) (statusStr string, err error) {
	payload, err := s.coordClient.StatusCheck(ctx, spaceId)
	if err != nil {
		res.CoordinatorStatus = "error"
		res.Log = append(res.Log, fmt.Sprintf("coordinatorStatus: error: %s", err))
		return "error", err
	}
	status := payload.GetStatus()
	statusStr = coordStatusString(status)
	res.CoordinatorStatus = statusStr
	res.Log = append(res.Log, fmt.Sprintf("coordinatorStatus: %s", statusStr))
	return statusStr, nil
}

func (s *spaceChecker) validate(res *Result, localStr string, coordStr string) {
	coordStatus := coordStr
	localStatus := localStr
	isResponsible := res.IsResponsible
	storageExists := res.SpaceStorageExists

	valid := false
	switch {
	// coordStatus: ok, localStatus: ok, isResponsible: true, storageExists: true
	case coordStatus == "ok" && localStatus == "ok" && isResponsible && storageExists:
		valid = true
	// coordStatus: ok, localStatus: archived, isResponsible: true, storageExists: false
	case coordStatus == "ok" && localStatus == "archived" && isResponsible && !storageExists:
		valid = true
	// coordStatus: remPrepare, localStatus: remPrepare, isResponsible: true, storageExists: true
	case coordStatus == "remPrepare" && localStatus == "remPrepare" && isResponsible && storageExists:
		valid = true
	// coordStatus: remPrepare, localStatus: remPrepare, isResponsible: false, storageExists: false
	case coordStatus == "remPrepare" && localStatus == "remPrepare" && !isResponsible && !storageExists:
		valid = true
	// coordStatus: removed, localStatus: removed, storageExists: false
	case coordStatus == "removed" && localStatus == "removed" && !storageExists:
		valid = true
	}

	if !valid {
		var problems []string
		if coordStatus == "error" {
			problems = append(problems, "coordinator_error")
		}
		if localStatus == "unknown" {
			problems = append(problems, "local_status_unknown")
		}
		problems = append(problems, fmt.Sprintf("invalid_state(C:%s,L:%s,R:%v,E:%v)", coordStatus, localStatus, isResponsible, storageExists))
		res.Problems = problems
	}
}

func localStatusString(s nodestorage.SpaceStatus) string {
	switch s {
	case nodestorage.SpaceStatusOk:
		return "ok"
	case nodestorage.SpaceStatusRemove:
		return "removed"
	case nodestorage.SpaceStatusRemovePrepare:
		return "remPrepare"
	case nodestorage.SpaceStatusArchived:
		return "archived"
	case nodestorage.SpaceStatusError:
		return "error"
	case nodestorage.SpaceStatusNotResponsible:
		return "notResponsible"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

func coordStatusString(s coordinatorproto.SpaceStatus) string {
	switch s {
	case coordinatorproto.SpaceStatus_SpaceStatusCreated:
		return "ok"
	case coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion:
		return "remPrepare"
	case coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted:
		return "remPrepare"
	case coordinatorproto.SpaceStatus_SpaceStatusDeleted:
		return "removed"
	case coordinatorproto.SpaceStatus_SpaceStatusNotExists:
		return "notExists"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}
