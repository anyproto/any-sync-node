// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/anytypeio/any-sync-node/nodesync/coldsync (interfaces: ColdSync)

// Package mock_coldsync is a generated GoMock package.
package mock_coldsync

import (
	context "context"
	reflect "reflect"

	nodesyncproto "github.com/anytypeio/any-sync-node/nodesync/nodesyncproto"
	app "github.com/anytypeio/any-sync/app"
	gomock "github.com/golang/mock/gomock"
)

// MockColdSync is a mock of ColdSync interface.
type MockColdSync struct {
	ctrl     *gomock.Controller
	recorder *MockColdSyncMockRecorder
}

// MockColdSyncMockRecorder is the mock recorder for MockColdSync.
type MockColdSyncMockRecorder struct {
	mock *MockColdSync
}

// NewMockColdSync creates a new mock instance.
func NewMockColdSync(ctrl *gomock.Controller) *MockColdSync {
	mock := &MockColdSync{ctrl: ctrl}
	mock.recorder = &MockColdSyncMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockColdSync) EXPECT() *MockColdSyncMockRecorder {
	return m.recorder
}

// ColdSyncHandle mocks base method.
func (m *MockColdSync) ColdSyncHandle(arg0 *nodesyncproto.ColdSyncRequest, arg1 nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ColdSyncHandle", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ColdSyncHandle indicates an expected call of ColdSyncHandle.
func (mr *MockColdSyncMockRecorder) ColdSyncHandle(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ColdSyncHandle", reflect.TypeOf((*MockColdSync)(nil).ColdSyncHandle), arg0, arg1)
}

// Init mocks base method.
func (m *MockColdSync) Init(arg0 *app.App) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockColdSyncMockRecorder) Init(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockColdSync)(nil).Init), arg0)
}

// Name mocks base method.
func (m *MockColdSync) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockColdSyncMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockColdSync)(nil).Name))
}

// Sync mocks base method.
func (m *MockColdSync) Sync(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Sync", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Sync indicates an expected call of Sync.
func (mr *MockColdSyncMockRecorder) Sync(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Sync", reflect.TypeOf((*MockColdSync)(nil).Sync), arg0, arg1, arg2)
}
