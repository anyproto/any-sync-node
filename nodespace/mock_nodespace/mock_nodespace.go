// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/anyproto/any-sync-node/nodespace (interfaces: Service,NodeSpace)
//
// Generated by this command:
//
//	mockgen -destination mock_nodespace/mock_nodespace.go github.com/anyproto/any-sync-node/nodespace Service,NodeSpace
//
// Package mock_nodespace is a generated GoMock package.
package mock_nodespace

import (
	context "context"
	reflect "reflect"
	time "time"

	nodespace "github.com/anyproto/any-sync-node/nodespace"
	nodestorage "github.com/anyproto/any-sync-node/nodestorage"
	app "github.com/anyproto/any-sync/app"
	ocache "github.com/anyproto/any-sync/app/ocache"
	commonspace "github.com/anyproto/any-sync/commonspace"
	aclclient "github.com/anyproto/any-sync/commonspace/acl/aclclient"
	headsync "github.com/anyproto/any-sync/commonspace/headsync"
	syncacl "github.com/anyproto/any-sync/commonspace/object/acl/syncacl"
	treesyncer "github.com/anyproto/any-sync/commonspace/object/treesyncer"
	objecttreebuilder "github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	spacestorage "github.com/anyproto/any-sync/commonspace/spacestorage"
	spacesyncproto "github.com/anyproto/any-sync/commonspace/spacesyncproto"
	objectmessages "github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	syncstatus "github.com/anyproto/any-sync/commonspace/syncstatus"
	peer "github.com/anyproto/any-sync/net/peer"
	gomock "go.uber.org/mock/gomock"
	drpc "storj.io/drpc"
)

// MockService is a mock of Service interface.
type MockService struct {
	ctrl     *gomock.Controller
	recorder *MockServiceMockRecorder
}

// MockServiceMockRecorder is the mock recorder for MockService.
type MockServiceMockRecorder struct {
	mock *MockService
}

// NewMockService creates a new mock instance.
func NewMockService(ctrl *gomock.Controller) *MockService {
	mock := &MockService{ctrl: ctrl}
	mock.recorder = &MockServiceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockService) EXPECT() *MockServiceMockRecorder {
	return m.recorder
}

// Cache mocks base method.
func (m *MockService) Cache() ocache.OCache {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cache")
	ret0, _ := ret[0].(ocache.OCache)
	return ret0
}

// Cache indicates an expected call of Cache.
func (mr *MockServiceMockRecorder) Cache() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cache", reflect.TypeOf((*MockService)(nil).Cache))
}

// Close mocks base method.
func (m *MockService) Close(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockServiceMockRecorder) Close(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockService)(nil).Close), arg0)
}

// EvictSpace mocks base method.
func (m *MockService) EvictSpace(arg0 context.Context, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EvictSpace", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// EvictSpace indicates an expected call of EvictSpace.
func (mr *MockServiceMockRecorder) EvictSpace(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EvictSpace", reflect.TypeOf((*MockService)(nil).EvictSpace), arg0, arg1)
}

// GetSpace mocks base method.
func (m *MockService) GetSpace(arg0 context.Context, arg1 string) (nodespace.NodeSpace, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSpace", arg0, arg1)
	ret0, _ := ret[0].(nodespace.NodeSpace)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSpace indicates an expected call of GetSpace.
func (mr *MockServiceMockRecorder) GetSpace(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSpace", reflect.TypeOf((*MockService)(nil).GetSpace), arg0, arg1)
}

// GetStats mocks base method.
func (m *MockService) GetStats(arg0 context.Context, arg1 string, arg2 int) (nodestorage.SpaceStats, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStats", arg0, arg1, arg2)
	ret0, _ := ret[0].(nodestorage.SpaceStats)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStats indicates an expected call of GetStats.
func (mr *MockServiceMockRecorder) GetStats(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStats", reflect.TypeOf((*MockService)(nil).GetStats), arg0, arg1, arg2)
}

// Init mocks base method.
func (m *MockService) Init(arg0 *app.App) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockServiceMockRecorder) Init(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockService)(nil).Init), arg0)
}

// Name mocks base method.
func (m *MockService) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockServiceMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockService)(nil).Name))
}

// PickSpace mocks base method.
func (m *MockService) PickSpace(arg0 context.Context, arg1 string) (nodespace.NodeSpace, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PickSpace", arg0, arg1)
	ret0, _ := ret[0].(nodespace.NodeSpace)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PickSpace indicates an expected call of PickSpace.
func (mr *MockServiceMockRecorder) PickSpace(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PickSpace", reflect.TypeOf((*MockService)(nil).PickSpace), arg0, arg1)
}

// Run mocks base method.
func (m *MockService) Run(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockServiceMockRecorder) Run(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockService)(nil).Run), arg0)
}

// MockNodeSpace is a mock of NodeSpace interface.
type MockNodeSpace struct {
	ctrl     *gomock.Controller
	recorder *MockNodeSpaceMockRecorder
}

// MockNodeSpaceMockRecorder is the mock recorder for MockNodeSpace.
type MockNodeSpaceMockRecorder struct {
	mock *MockNodeSpace
}

// NewMockNodeSpace creates a new mock instance.
func NewMockNodeSpace(ctrl *gomock.Controller) *MockNodeSpace {
	mock := &MockNodeSpace{ctrl: ctrl}
	mock.recorder = &MockNodeSpaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNodeSpace) EXPECT() *MockNodeSpaceMockRecorder {
	return m.recorder
}

// Acl mocks base method.
func (m *MockNodeSpace) Acl() syncacl.SyncAcl {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Acl")
	ret0, _ := ret[0].(syncacl.SyncAcl)
	return ret0
}

// Acl indicates an expected call of Acl.
func (mr *MockNodeSpaceMockRecorder) Acl() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Acl", reflect.TypeOf((*MockNodeSpace)(nil).Acl))
}

// AclClient mocks base method.
func (m *MockNodeSpace) AclClient() aclclient.AclSpaceClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AclClient")
	ret0, _ := ret[0].(aclclient.AclSpaceClient)
	return ret0
}

// AclClient indicates an expected call of AclClient.
func (mr *MockNodeSpaceMockRecorder) AclClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AclClient", reflect.TypeOf((*MockNodeSpace)(nil).AclClient))
}

// Close mocks base method.
func (m *MockNodeSpace) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockNodeSpaceMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockNodeSpace)(nil).Close))
}

// DebugAllHeads mocks base method.
func (m *MockNodeSpace) DebugAllHeads() []headsync.TreeHeads {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DebugAllHeads")
	ret0, _ := ret[0].([]headsync.TreeHeads)
	return ret0
}

// DebugAllHeads indicates an expected call of DebugAllHeads.
func (mr *MockNodeSpaceMockRecorder) DebugAllHeads() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DebugAllHeads", reflect.TypeOf((*MockNodeSpace)(nil).DebugAllHeads))
}

// DeleteTree mocks base method.
func (m *MockNodeSpace) DeleteTree(arg0 context.Context, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteTree", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteTree indicates an expected call of DeleteTree.
func (mr *MockNodeSpaceMockRecorder) DeleteTree(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTree", reflect.TypeOf((*MockNodeSpace)(nil).DeleteTree), arg0, arg1)
}

// Description mocks base method.
func (m *MockNodeSpace) Description(arg0 context.Context) (commonspace.SpaceDescription, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Description", arg0)
	ret0, _ := ret[0].(commonspace.SpaceDescription)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Description indicates an expected call of Description.
func (mr *MockNodeSpaceMockRecorder) Description(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Description", reflect.TypeOf((*MockNodeSpace)(nil).Description), arg0)
}

// GetNodePeers mocks base method.
func (m *MockNodeSpace) GetNodePeers(arg0 context.Context) ([]peer.Peer, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNodePeers", arg0)
	ret0, _ := ret[0].([]peer.Peer)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNodePeers indicates an expected call of GetNodePeers.
func (mr *MockNodeSpaceMockRecorder) GetNodePeers(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNodePeers", reflect.TypeOf((*MockNodeSpace)(nil).GetNodePeers), arg0)
}

// HandleMessage mocks base method.
func (m *MockNodeSpace) HandleMessage(arg0 context.Context, arg1 *objectmessages.HeadUpdate) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleMessage", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleMessage indicates an expected call of HandleMessage.
func (mr *MockNodeSpaceMockRecorder) HandleMessage(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleMessage", reflect.TypeOf((*MockNodeSpace)(nil).HandleMessage), arg0, arg1)
}

// HandleRangeRequest mocks base method.
func (m *MockNodeSpace) HandleRangeRequest(arg0 context.Context, arg1 *spacesyncproto.HeadSyncRequest) (*spacesyncproto.HeadSyncResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleRangeRequest", arg0, arg1)
	ret0, _ := ret[0].(*spacesyncproto.HeadSyncResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HandleRangeRequest indicates an expected call of HandleRangeRequest.
func (mr *MockNodeSpaceMockRecorder) HandleRangeRequest(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleRangeRequest", reflect.TypeOf((*MockNodeSpace)(nil).HandleRangeRequest), arg0, arg1)
}

// HandleStreamSyncRequest mocks base method.
func (m *MockNodeSpace) HandleStreamSyncRequest(arg0 context.Context, arg1 *spacesyncproto.ObjectSyncMessage, arg2 drpc.Stream) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleStreamSyncRequest", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleStreamSyncRequest indicates an expected call of HandleStreamSyncRequest.
func (mr *MockNodeSpaceMockRecorder) HandleStreamSyncRequest(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleStreamSyncRequest", reflect.TypeOf((*MockNodeSpace)(nil).HandleStreamSyncRequest), arg0, arg1, arg2)
}

// Id mocks base method.
func (m *MockNodeSpace) Id() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Id")
	ret0, _ := ret[0].(string)
	return ret0
}

// Id indicates an expected call of Id.
func (mr *MockNodeSpaceMockRecorder) Id() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Id", reflect.TypeOf((*MockNodeSpace)(nil).Id))
}

// Init mocks base method.
func (m *MockNodeSpace) Init(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockNodeSpaceMockRecorder) Init(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockNodeSpace)(nil).Init), arg0)
}

// Storage mocks base method.
func (m *MockNodeSpace) Storage() spacestorage.SpaceStorage {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Storage")
	ret0, _ := ret[0].(spacestorage.SpaceStorage)
	return ret0
}

// Storage indicates an expected call of Storage.
func (mr *MockNodeSpaceMockRecorder) Storage() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Storage", reflect.TypeOf((*MockNodeSpace)(nil).Storage))
}

// StoredIds mocks base method.
func (m *MockNodeSpace) StoredIds() []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoredIds")
	ret0, _ := ret[0].([]string)
	return ret0
}

// StoredIds indicates an expected call of StoredIds.
func (mr *MockNodeSpaceMockRecorder) StoredIds() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoredIds", reflect.TypeOf((*MockNodeSpace)(nil).StoredIds))
}

// SyncStatus mocks base method.
func (m *MockNodeSpace) SyncStatus() syncstatus.StatusUpdater {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SyncStatus")
	ret0, _ := ret[0].(syncstatus.StatusUpdater)
	return ret0
}

// SyncStatus indicates an expected call of SyncStatus.
func (mr *MockNodeSpaceMockRecorder) SyncStatus() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncStatus", reflect.TypeOf((*MockNodeSpace)(nil).SyncStatus))
}

// TreeBuilder mocks base method.
func (m *MockNodeSpace) TreeBuilder() objecttreebuilder.TreeBuilder {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TreeBuilder")
	ret0, _ := ret[0].(objecttreebuilder.TreeBuilder)
	return ret0
}

// TreeBuilder indicates an expected call of TreeBuilder.
func (mr *MockNodeSpaceMockRecorder) TreeBuilder() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TreeBuilder", reflect.TypeOf((*MockNodeSpace)(nil).TreeBuilder))
}

// TreeSyncer mocks base method.
func (m *MockNodeSpace) TreeSyncer() treesyncer.TreeSyncer {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TreeSyncer")
	ret0, _ := ret[0].(treesyncer.TreeSyncer)
	return ret0
}

// TreeSyncer indicates an expected call of TreeSyncer.
func (mr *MockNodeSpaceMockRecorder) TreeSyncer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TreeSyncer", reflect.TypeOf((*MockNodeSpace)(nil).TreeSyncer))
}

// TryClose mocks base method.
func (m *MockNodeSpace) TryClose(arg0 time.Duration) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TryClose", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TryClose indicates an expected call of TryClose.
func (mr *MockNodeSpaceMockRecorder) TryClose(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TryClose", reflect.TypeOf((*MockNodeSpace)(nil).TryClose), arg0)
}
