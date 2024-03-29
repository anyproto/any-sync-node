// Code generated by protoc-gen-go-drpc. DO NOT EDIT.
// protoc-gen-go-drpc version: v0.0.32
// source: debug/nodedebugrpc/nodedebugrpcproto/protos/nodedebugrpc.proto

package nodedebugrpcproto

import (
	bytes "bytes"
	context "context"
	errors "errors"
	jsonpb "github.com/gogo/protobuf/jsonpb"
	proto "github.com/gogo/protobuf/proto"
	drpc "storj.io/drpc"
	drpcerr "storj.io/drpc/drpcerr"
)

type drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto struct{}

func (drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto) Marshal(msg drpc.Message) ([]byte, error) {
	return proto.Marshal(msg.(proto.Message))
}

func (drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto) Unmarshal(buf []byte, msg drpc.Message) error {
	return proto.Unmarshal(buf, msg.(proto.Message))
}

func (drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto) JSONMarshal(msg drpc.Message) ([]byte, error) {
	var buf bytes.Buffer
	err := new(jsonpb.Marshaler).Marshal(&buf, msg.(proto.Message))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto) JSONUnmarshal(buf []byte, msg drpc.Message) error {
	return jsonpb.Unmarshal(bytes.NewReader(buf), msg.(proto.Message))
}

type DRPCNodeApiClient interface {
	DRPCConn() drpc.Conn

	DumpTree(ctx context.Context, in *DumpTreeRequest) (*DumpTreeResponse, error)
	TreeParams(ctx context.Context, in *TreeParamsRequest) (*TreeParamsResponse, error)
	AllTrees(ctx context.Context, in *AllTreesRequest) (*AllTreesResponse, error)
	AllSpaces(ctx context.Context, in *AllSpacesRequest) (*AllSpacesResponse, error)
	ForceNodeSync(ctx context.Context, in *ForceNodeSyncRequest) (*ForceNodeSyncResponse, error)
	NodesAddressesBySpace(ctx context.Context, in *NodesAddressesBySpaceRequest) (*NodesAddressesBySpaceResponse, error)
}

type drpcNodeApiClient struct {
	cc drpc.Conn
}

func NewDRPCNodeApiClient(cc drpc.Conn) DRPCNodeApiClient {
	return &drpcNodeApiClient{cc}
}

func (c *drpcNodeApiClient) DRPCConn() drpc.Conn { return c.cc }

func (c *drpcNodeApiClient) DumpTree(ctx context.Context, in *DumpTreeRequest) (*DumpTreeResponse, error) {
	out := new(DumpTreeResponse)
	err := c.cc.Invoke(ctx, "/nodeapi.NodeApi/DumpTree", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}, in, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *drpcNodeApiClient) TreeParams(ctx context.Context, in *TreeParamsRequest) (*TreeParamsResponse, error) {
	out := new(TreeParamsResponse)
	err := c.cc.Invoke(ctx, "/nodeapi.NodeApi/TreeParams", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}, in, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *drpcNodeApiClient) AllTrees(ctx context.Context, in *AllTreesRequest) (*AllTreesResponse, error) {
	out := new(AllTreesResponse)
	err := c.cc.Invoke(ctx, "/nodeapi.NodeApi/AllTrees", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}, in, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *drpcNodeApiClient) AllSpaces(ctx context.Context, in *AllSpacesRequest) (*AllSpacesResponse, error) {
	out := new(AllSpacesResponse)
	err := c.cc.Invoke(ctx, "/nodeapi.NodeApi/AllSpaces", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}, in, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *drpcNodeApiClient) ForceNodeSync(ctx context.Context, in *ForceNodeSyncRequest) (*ForceNodeSyncResponse, error) {
	out := new(ForceNodeSyncResponse)
	err := c.cc.Invoke(ctx, "/nodeapi.NodeApi/ForceNodeSync", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}, in, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *drpcNodeApiClient) NodesAddressesBySpace(ctx context.Context, in *NodesAddressesBySpaceRequest) (*NodesAddressesBySpaceResponse, error) {
	out := new(NodesAddressesBySpaceResponse)
	err := c.cc.Invoke(ctx, "/nodeapi.NodeApi/NodesAddressesBySpace", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}, in, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type DRPCNodeApiServer interface {
	DumpTree(context.Context, *DumpTreeRequest) (*DumpTreeResponse, error)
	TreeParams(context.Context, *TreeParamsRequest) (*TreeParamsResponse, error)
	AllTrees(context.Context, *AllTreesRequest) (*AllTreesResponse, error)
	AllSpaces(context.Context, *AllSpacesRequest) (*AllSpacesResponse, error)
	ForceNodeSync(context.Context, *ForceNodeSyncRequest) (*ForceNodeSyncResponse, error)
	NodesAddressesBySpace(context.Context, *NodesAddressesBySpaceRequest) (*NodesAddressesBySpaceResponse, error)
}

type DRPCNodeApiUnimplementedServer struct{}

func (s *DRPCNodeApiUnimplementedServer) DumpTree(context.Context, *DumpTreeRequest) (*DumpTreeResponse, error) {
	return nil, drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

func (s *DRPCNodeApiUnimplementedServer) TreeParams(context.Context, *TreeParamsRequest) (*TreeParamsResponse, error) {
	return nil, drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

func (s *DRPCNodeApiUnimplementedServer) AllTrees(context.Context, *AllTreesRequest) (*AllTreesResponse, error) {
	return nil, drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

func (s *DRPCNodeApiUnimplementedServer) AllSpaces(context.Context, *AllSpacesRequest) (*AllSpacesResponse, error) {
	return nil, drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

func (s *DRPCNodeApiUnimplementedServer) ForceNodeSync(context.Context, *ForceNodeSyncRequest) (*ForceNodeSyncResponse, error) {
	return nil, drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

func (s *DRPCNodeApiUnimplementedServer) NodesAddressesBySpace(context.Context, *NodesAddressesBySpaceRequest) (*NodesAddressesBySpaceResponse, error) {
	return nil, drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

type DRPCNodeApiDescription struct{}

func (DRPCNodeApiDescription) NumMethods() int { return 6 }

func (DRPCNodeApiDescription) Method(n int) (string, drpc.Encoding, drpc.Receiver, interface{}, bool) {
	switch n {
	case 0:
		return "/nodeapi.NodeApi/DumpTree", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return srv.(DRPCNodeApiServer).
					DumpTree(
						ctx,
						in1.(*DumpTreeRequest),
					)
			}, DRPCNodeApiServer.DumpTree, true
	case 1:
		return "/nodeapi.NodeApi/TreeParams", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return srv.(DRPCNodeApiServer).
					TreeParams(
						ctx,
						in1.(*TreeParamsRequest),
					)
			}, DRPCNodeApiServer.TreeParams, true
	case 2:
		return "/nodeapi.NodeApi/AllTrees", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return srv.(DRPCNodeApiServer).
					AllTrees(
						ctx,
						in1.(*AllTreesRequest),
					)
			}, DRPCNodeApiServer.AllTrees, true
	case 3:
		return "/nodeapi.NodeApi/AllSpaces", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return srv.(DRPCNodeApiServer).
					AllSpaces(
						ctx,
						in1.(*AllSpacesRequest),
					)
			}, DRPCNodeApiServer.AllSpaces, true
	case 4:
		return "/nodeapi.NodeApi/ForceNodeSync", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return srv.(DRPCNodeApiServer).
					ForceNodeSync(
						ctx,
						in1.(*ForceNodeSyncRequest),
					)
			}, DRPCNodeApiServer.ForceNodeSync, true
	case 5:
		return "/nodeapi.NodeApi/NodesAddressesBySpace", drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return srv.(DRPCNodeApiServer).
					NodesAddressesBySpace(
						ctx,
						in1.(*NodesAddressesBySpaceRequest),
					)
			}, DRPCNodeApiServer.NodesAddressesBySpace, true
	default:
		return "", nil, nil, nil, false
	}
}

func DRPCRegisterNodeApi(mux drpc.Mux, impl DRPCNodeApiServer) error {
	return mux.Register(impl, DRPCNodeApiDescription{})
}

type DRPCNodeApi_DumpTreeStream interface {
	drpc.Stream
	SendAndClose(*DumpTreeResponse) error
}

type drpcNodeApi_DumpTreeStream struct {
	drpc.Stream
}

func (x *drpcNodeApi_DumpTreeStream) SendAndClose(m *DumpTreeResponse) error {
	if err := x.MsgSend(m, drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}); err != nil {
		return err
	}
	return x.CloseSend()
}

type DRPCNodeApi_TreeParamsStream interface {
	drpc.Stream
	SendAndClose(*TreeParamsResponse) error
}

type drpcNodeApi_TreeParamsStream struct {
	drpc.Stream
}

func (x *drpcNodeApi_TreeParamsStream) SendAndClose(m *TreeParamsResponse) error {
	if err := x.MsgSend(m, drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}); err != nil {
		return err
	}
	return x.CloseSend()
}

type DRPCNodeApi_AllTreesStream interface {
	drpc.Stream
	SendAndClose(*AllTreesResponse) error
}

type drpcNodeApi_AllTreesStream struct {
	drpc.Stream
}

func (x *drpcNodeApi_AllTreesStream) SendAndClose(m *AllTreesResponse) error {
	if err := x.MsgSend(m, drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}); err != nil {
		return err
	}
	return x.CloseSend()
}

type DRPCNodeApi_AllSpacesStream interface {
	drpc.Stream
	SendAndClose(*AllSpacesResponse) error
}

type drpcNodeApi_AllSpacesStream struct {
	drpc.Stream
}

func (x *drpcNodeApi_AllSpacesStream) SendAndClose(m *AllSpacesResponse) error {
	if err := x.MsgSend(m, drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}); err != nil {
		return err
	}
	return x.CloseSend()
}

type DRPCNodeApi_ForceNodeSyncStream interface {
	drpc.Stream
	SendAndClose(*ForceNodeSyncResponse) error
}

type drpcNodeApi_ForceNodeSyncStream struct {
	drpc.Stream
}

func (x *drpcNodeApi_ForceNodeSyncStream) SendAndClose(m *ForceNodeSyncResponse) error {
	if err := x.MsgSend(m, drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}); err != nil {
		return err
	}
	return x.CloseSend()
}

type DRPCNodeApi_NodesAddressesBySpaceStream interface {
	drpc.Stream
	SendAndClose(*NodesAddressesBySpaceResponse) error
}

type drpcNodeApi_NodesAddressesBySpaceStream struct {
	drpc.Stream
}

func (x *drpcNodeApi_NodesAddressesBySpaceStream) SendAndClose(m *NodesAddressesBySpaceResponse) error {
	if err := x.MsgSend(m, drpcEncoding_File_debug_nodedebugrpc_nodedebugrpcproto_protos_nodedebugrpc_proto{}); err != nil {
		return err
	}
	return x.CloseSend()
}
