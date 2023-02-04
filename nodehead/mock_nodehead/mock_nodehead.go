// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/anytypeio/any-sync-node/nodehead (interfaces: NodeHead)

// Package mock_nodehead is a generated GoMock package.
package mock_nodehead

import (
	context "context"
	reflect "reflect"

	app "github.com/anytypeio/any-sync/app"
	ldiff "github.com/anytypeio/any-sync/app/ldiff"
	gomock "github.com/golang/mock/gomock"
)

// MockNodeHead is a mock of NodeHead interface.
type MockNodeHead struct {
	ctrl     *gomock.Controller
	recorder *MockNodeHeadMockRecorder
}

// MockNodeHeadMockRecorder is the mock recorder for MockNodeHead.
type MockNodeHeadMockRecorder struct {
	mock *MockNodeHead
}

// NewMockNodeHead creates a new mock instance.
func NewMockNodeHead(ctrl *gomock.Controller) *MockNodeHead {
	mock := &MockNodeHead{ctrl: ctrl}
	mock.recorder = &MockNodeHeadMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNodeHead) EXPECT() *MockNodeHeadMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockNodeHead) Close(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockNodeHeadMockRecorder) Close(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockNodeHead)(nil).Close), arg0)
}

// GetHead mocks base method.
func (m *MockNodeHead) GetHead(arg0 string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHead", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetHead indicates an expected call of GetHead.
func (mr *MockNodeHeadMockRecorder) GetHead(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHead", reflect.TypeOf((*MockNodeHead)(nil).GetHead), arg0)
}

// Init mocks base method.
func (m *MockNodeHead) Init(arg0 *app.App) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockNodeHeadMockRecorder) Init(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockNodeHead)(nil).Init), arg0)
}

// LDiff mocks base method.
func (m *MockNodeHead) LDiff(arg0 int) ldiff.Diff {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LDiff", arg0)
	ret0, _ := ret[0].(ldiff.Diff)
	return ret0
}

// LDiff indicates an expected call of LDiff.
func (mr *MockNodeHeadMockRecorder) LDiff(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LDiff", reflect.TypeOf((*MockNodeHead)(nil).LDiff), arg0)
}

// Name mocks base method.
func (m *MockNodeHead) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockNodeHeadMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockNodeHead)(nil).Name))
}

// Ranges mocks base method.
func (m *MockNodeHead) Ranges(arg0 context.Context, arg1 int, arg2 []ldiff.Range, arg3 []ldiff.RangeResult) ([]ldiff.RangeResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ranges", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]ldiff.RangeResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Ranges indicates an expected call of Ranges.
func (mr *MockNodeHeadMockRecorder) Ranges(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ranges", reflect.TypeOf((*MockNodeHead)(nil).Ranges), arg0, arg1, arg2, arg3)
}

// ReloadHeadFromStore mocks base method.
func (m *MockNodeHead) ReloadHeadFromStore(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReloadHeadFromStore", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReloadHeadFromStore indicates an expected call of ReloadHeadFromStore.
func (mr *MockNodeHeadMockRecorder) ReloadHeadFromStore(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReloadHeadFromStore", reflect.TypeOf((*MockNodeHead)(nil).ReloadHeadFromStore), arg0)
}

// Run mocks base method.
func (m *MockNodeHead) Run(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockNodeHeadMockRecorder) Run(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockNodeHead)(nil).Run), arg0)
}

// SetHead mocks base method.
func (m *MockNodeHead) SetHead(arg0, arg1 string) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetHead", arg0, arg1)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SetHead indicates an expected call of SetHead.
func (mr *MockNodeHeadMockRecorder) SetHead(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetHead", reflect.TypeOf((*MockNodeHead)(nil).SetHead), arg0, arg1)
}
