// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package icecanedbpb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// IcecaneKVClient is the client API for IcecaneKV service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type IcecaneKVClient interface {
	// KV commands with txn supported.
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
	BeginTxn(ctx context.Context, in *BeginTxnRequest, opts ...grpc.CallOption) (*BeginTxnResponse, error)
	CommitTxn(ctx context.Context, in *CommitTxnRequest, opts ...grpc.CallOption) (*CommitTxnResponse, error)
	RollbackTxn(ctx context.Context, in *RollbackTxnRequest, opts ...grpc.CallOption) (*RollbackTxnResponse, error)
	// RawKV commands.
	RawGet(ctx context.Context, in *RawGetRequest, opts ...grpc.CallOption) (*RawGetResponse, error)
	RawPut(ctx context.Context, in *RawPutRequest, opts ...grpc.CallOption) (*RawPutResponse, error)
	RawDelete(ctx context.Context, in *RawDeleteRequest, opts ...grpc.CallOption) (*RawDeleteResponse, error)
	// Raft commands b/w two kv servers.
	RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error)
}

type icecaneKVClient struct {
	cc grpc.ClientConnInterface
}

func NewIcecaneKVClient(cc grpc.ClientConnInterface) IcecaneKVClient {
	return &icecaneKVClient{cc}
}

func (c *icecaneKVClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) BeginTxn(ctx context.Context, in *BeginTxnRequest, opts ...grpc.CallOption) (*BeginTxnResponse, error) {
	out := new(BeginTxnResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/BeginTxn", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) CommitTxn(ctx context.Context, in *CommitTxnRequest, opts ...grpc.CallOption) (*CommitTxnResponse, error) {
	out := new(CommitTxnResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/CommitTxn", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) RollbackTxn(ctx context.Context, in *RollbackTxnRequest, opts ...grpc.CallOption) (*RollbackTxnResponse, error) {
	out := new(RollbackTxnResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/RollbackTxn", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) RawGet(ctx context.Context, in *RawGetRequest, opts ...grpc.CallOption) (*RawGetResponse, error) {
	out := new(RawGetResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/RawGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) RawPut(ctx context.Context, in *RawPutRequest, opts ...grpc.CallOption) (*RawPutResponse, error) {
	out := new(RawPutResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/RawPut", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) RawDelete(ctx context.Context, in *RawDeleteRequest, opts ...grpc.CallOption) (*RawDeleteResponse, error) {
	out := new(RawDeleteResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/RawDelete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error) {
	out := new(RequestVoteResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/RequestVote", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *icecaneKVClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	out := new(AppendEntriesResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/AppendEntries", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// IcecaneKVServer is the server API for IcecaneKV service.
// All implementations must embed UnimplementedIcecaneKVServer
// for forward compatibility
type IcecaneKVServer interface {
	// KV commands with txn supported.
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	BeginTxn(context.Context, *BeginTxnRequest) (*BeginTxnResponse, error)
	CommitTxn(context.Context, *CommitTxnRequest) (*CommitTxnResponse, error)
	RollbackTxn(context.Context, *RollbackTxnRequest) (*RollbackTxnResponse, error)
	// RawKV commands.
	RawGet(context.Context, *RawGetRequest) (*RawGetResponse, error)
	RawPut(context.Context, *RawPutRequest) (*RawPutResponse, error)
	RawDelete(context.Context, *RawDeleteRequest) (*RawDeleteResponse, error)
	// Raft commands b/w two kv servers.
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	mustEmbedUnimplementedIcecaneKVServer()
}

// UnimplementedIcecaneKVServer must be embedded to have forward compatible implementations.
type UnimplementedIcecaneKVServer struct {
}

func (UnimplementedIcecaneKVServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedIcecaneKVServer) Put(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
func (UnimplementedIcecaneKVServer) Delete(context.Context, *DeleteRequest) (*DeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Delete not implemented")
}
func (UnimplementedIcecaneKVServer) BeginTxn(context.Context, *BeginTxnRequest) (*BeginTxnResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BeginTxn not implemented")
}
func (UnimplementedIcecaneKVServer) CommitTxn(context.Context, *CommitTxnRequest) (*CommitTxnResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CommitTxn not implemented")
}
func (UnimplementedIcecaneKVServer) RollbackTxn(context.Context, *RollbackTxnRequest) (*RollbackTxnResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RollbackTxn not implemented")
}
func (UnimplementedIcecaneKVServer) RawGet(context.Context, *RawGetRequest) (*RawGetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}
func (UnimplementedIcecaneKVServer) RawPut(context.Context, *RawPutRequest) (*RawPutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawPut not implemented")
}
func (UnimplementedIcecaneKVServer) RawDelete(context.Context, *RawDeleteRequest) (*RawDeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawDelete not implemented")
}
func (UnimplementedIcecaneKVServer) RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (UnimplementedIcecaneKVServer) AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntries not implemented")
}
func (UnimplementedIcecaneKVServer) mustEmbedUnimplementedIcecaneKVServer() {}

// UnsafeIcecaneKVServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to IcecaneKVServer will
// result in compilation errors.
type UnsafeIcecaneKVServer interface {
	mustEmbedUnimplementedIcecaneKVServer()
}

func RegisterIcecaneKVServer(s grpc.ServiceRegistrar, srv IcecaneKVServer) {
	s.RegisterService(&IcecaneKV_ServiceDesc, srv)
}

func _IcecaneKV_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_BeginTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BeginTxnRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).BeginTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/BeginTxn",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).BeginTxn(ctx, req.(*BeginTxnRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_CommitTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CommitTxnRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).CommitTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/CommitTxn",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).CommitTxn(ctx, req.(*CommitTxnRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_RollbackTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RollbackTxnRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).RollbackTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/RollbackTxn",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).RollbackTxn(ctx, req.(*RollbackTxnRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_RawGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RawGetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).RawGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/RawGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).RawGet(ctx, req.(*RawGetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_RawPut_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RawPutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).RawPut(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/RawPut",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).RawPut(ctx, req.(*RawPutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_RawDelete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RawDeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).RawDelete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/RawDelete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).RawDelete(ctx, req.(*RawDeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/RequestVote",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).RequestVote(ctx, req.(*RequestVoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IcecaneKV_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IcecaneKVServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/icecanedbpb.IcecaneKV/AppendEntries",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IcecaneKVServer).AppendEntries(ctx, req.(*AppendEntriesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// IcecaneKV_ServiceDesc is the grpc.ServiceDesc for IcecaneKV service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var IcecaneKV_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "icecanedbpb.IcecaneKV",
	HandlerType: (*IcecaneKVServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _IcecaneKV_Get_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _IcecaneKV_Put_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _IcecaneKV_Delete_Handler,
		},
		{
			MethodName: "BeginTxn",
			Handler:    _IcecaneKV_BeginTxn_Handler,
		},
		{
			MethodName: "CommitTxn",
			Handler:    _IcecaneKV_CommitTxn_Handler,
		},
		{
			MethodName: "RollbackTxn",
			Handler:    _IcecaneKV_RollbackTxn_Handler,
		},
		{
			MethodName: "RawGet",
			Handler:    _IcecaneKV_RawGet_Handler,
		},
		{
			MethodName: "RawPut",
			Handler:    _IcecaneKV_RawPut_Handler,
		},
		{
			MethodName: "RawDelete",
			Handler:    _IcecaneKV_RawDelete_Handler,
		},
		{
			MethodName: "RequestVote",
			Handler:    _IcecaneKV_RequestVote_Handler,
		},
		{
			MethodName: "AppendEntries",
			Handler:    _IcecaneKV_AppendEntries_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "icecanekvpb.proto",
}
