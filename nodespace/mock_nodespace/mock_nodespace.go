// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/anytypeio/any-sync-node/nodespace (interfaces: Service)

// Package mock_nodespace is a generated GoMock package.
package mock_nodespace

import (
	context "context"
	reflect "reflect"

	app "github.com/anytypeio/any-sync/app"
	commonspace "github.com/anytypeio/any-sync/commonspace"
	streampool "github.com/anytypeio/any-sync/net/streampool"
	gomock "github.com/golang/mock/gomock"
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

// Close mocks base method.
func (m *MockService) Close(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockServiceMockRecorder) Close(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockService)(nil).Close), arg0)
}

// GetOrPickSpace mocks base method.
func (m *MockService) GetOrPickSpace(arg0 context.Context, arg1 string) (commonspace.Space, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOrPickSpace", arg0, arg1)
	ret0, _ := ret[0].(commonspace.Space)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOrPickSpace indicates an expected call of GetOrPickSpace.
func (mr *MockServiceMockRecorder) GetOrPickSpace(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrPickSpace", reflect.TypeOf((*MockService)(nil).GetOrPickSpace), arg0, arg1)
}

// GetSpace mocks base method.
func (m *MockService) GetSpace(arg0 context.Context, arg1 string) (commonspace.Space, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSpace", arg0, arg1)
	ret0, _ := ret[0].(commonspace.Space)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSpace indicates an expected call of GetSpace.
func (mr *MockServiceMockRecorder) GetSpace(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSpace", reflect.TypeOf((*MockService)(nil).GetSpace), arg0, arg1)
}

// Init mocks base method.
func (m *MockService) Init(arg0 *app.App) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockServiceMockRecorder) Init(arg0 interface{}) *gomock.Call {
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

// Run mocks base method.
func (m *MockService) Run(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockServiceMockRecorder) Run(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockService)(nil).Run), arg0)
}

// StreamPool mocks base method.
func (m *MockService) StreamPool() streampool.StreamPool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StreamPool")
	ret0, _ := ret[0].(streampool.StreamPool)
	return ret0
}

// StreamPool indicates an expected call of StreamPool.
func (mr *MockServiceMockRecorder) StreamPool() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StreamPool", reflect.TypeOf((*MockService)(nil).StreamPool))
}