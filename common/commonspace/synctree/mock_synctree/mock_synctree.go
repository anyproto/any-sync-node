// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/anytypeio/go-anytype-infrastructure-experiments/common/commonspace/synctree (interfaces: SyncClient)

// Package mock_synctree is a generated GoMock package.
package mock_synctree

import (
	reflect "reflect"
	time "time"

	tree "github.com/anytypeio/go-anytype-infrastructure-experiments/common/pkg/acl/tree"
	treechangeproto "github.com/anytypeio/go-anytype-infrastructure-experiments/common/pkg/acl/treechangeproto"
	gomock "github.com/golang/mock/gomock"
)

// MockSyncClient is a mock of SyncClient interface.
type MockSyncClient struct {
	ctrl     *gomock.Controller
	recorder *MockSyncClientMockRecorder
}

// MockSyncClientMockRecorder is the mock recorder for MockSyncClient.
type MockSyncClientMockRecorder struct {
	mock *MockSyncClient
}

// NewMockSyncClient creates a new mock instance.
func NewMockSyncClient(ctrl *gomock.Controller) *MockSyncClient {
	mock := &MockSyncClient{ctrl: ctrl}
	mock.recorder = &MockSyncClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSyncClient) EXPECT() *MockSyncClientMockRecorder {
	return m.recorder
}

// BroadcastAsync mocks base method.
func (m *MockSyncClient) BroadcastAsync(arg0 *treechangeproto.TreeSyncMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BroadcastAsync", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// BroadcastAsync indicates an expected call of BroadcastAsync.
func (mr *MockSyncClientMockRecorder) BroadcastAsync(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BroadcastAsync", reflect.TypeOf((*MockSyncClient)(nil).BroadcastAsync), arg0)
}

// BroadcastAsyncOrSendResponsible mocks base method.
func (m *MockSyncClient) BroadcastAsyncOrSendResponsible(arg0 *treechangeproto.TreeSyncMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BroadcastAsyncOrSendResponsible", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// BroadcastAsyncOrSendResponsible indicates an expected call of BroadcastAsyncOrSendResponsible.
func (mr *MockSyncClientMockRecorder) BroadcastAsyncOrSendResponsible(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BroadcastAsyncOrSendResponsible", reflect.TypeOf((*MockSyncClient)(nil).BroadcastAsyncOrSendResponsible), arg0)
}

// CreateFullSyncRequest mocks base method.
func (m *MockSyncClient) CreateFullSyncRequest(arg0 tree.ObjectTree, arg1, arg2 []string) (*treechangeproto.TreeSyncMessage, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateFullSyncRequest", arg0, arg1, arg2)
	ret0, _ := ret[0].(*treechangeproto.TreeSyncMessage)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateFullSyncRequest indicates an expected call of CreateFullSyncRequest.
func (mr *MockSyncClientMockRecorder) CreateFullSyncRequest(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateFullSyncRequest", reflect.TypeOf((*MockSyncClient)(nil).CreateFullSyncRequest), arg0, arg1, arg2)
}

// CreateFullSyncResponse mocks base method.
func (m *MockSyncClient) CreateFullSyncResponse(arg0 tree.ObjectTree, arg1, arg2 []string) (*treechangeproto.TreeSyncMessage, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateFullSyncResponse", arg0, arg1, arg2)
	ret0, _ := ret[0].(*treechangeproto.TreeSyncMessage)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateFullSyncResponse indicates an expected call of CreateFullSyncResponse.
func (mr *MockSyncClientMockRecorder) CreateFullSyncResponse(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateFullSyncResponse", reflect.TypeOf((*MockSyncClient)(nil).CreateFullSyncResponse), arg0, arg1, arg2)
}

// CreateHeadUpdate mocks base method.
func (m *MockSyncClient) CreateHeadUpdate(arg0 tree.ObjectTree, arg1 []*treechangeproto.RawTreeChangeWithId) *treechangeproto.TreeSyncMessage {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateHeadUpdate", arg0, arg1)
	ret0, _ := ret[0].(*treechangeproto.TreeSyncMessage)
	return ret0
}

// CreateHeadUpdate indicates an expected call of CreateHeadUpdate.
func (mr *MockSyncClientMockRecorder) CreateHeadUpdate(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateHeadUpdate", reflect.TypeOf((*MockSyncClient)(nil).CreateHeadUpdate), arg0, arg1)
}

// CreateNewTreeRequest mocks base method.
func (m *MockSyncClient) CreateNewTreeRequest() *treechangeproto.TreeSyncMessage {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateNewTreeRequest")
	ret0, _ := ret[0].(*treechangeproto.TreeSyncMessage)
	return ret0
}

// CreateNewTreeRequest indicates an expected call of CreateNewTreeRequest.
func (mr *MockSyncClientMockRecorder) CreateNewTreeRequest() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateNewTreeRequest", reflect.TypeOf((*MockSyncClient)(nil).CreateNewTreeRequest))
}

// LastUsage mocks base method.
func (m *MockSyncClient) LastUsage() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LastUsage")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// LastUsage indicates an expected call of LastUsage.
func (mr *MockSyncClientMockRecorder) LastUsage() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LastUsage", reflect.TypeOf((*MockSyncClient)(nil).LastUsage))
}

// SendAsync mocks base method.
func (m *MockSyncClient) SendAsync(arg0 string, arg1 *treechangeproto.TreeSyncMessage, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendAsync", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendAsync indicates an expected call of SendAsync.
func (mr *MockSyncClientMockRecorder) SendAsync(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendAsync", reflect.TypeOf((*MockSyncClient)(nil).SendAsync), arg0, arg1, arg2)
}