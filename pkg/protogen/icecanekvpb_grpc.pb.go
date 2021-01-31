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
const _ = grpc.SupportPackageIsVersion7

// IcecaneKVClient is the client API for IcecaneKV service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type IcecaneKVClient interface {
	// RawKV commands.
	RawGet(ctx context.Context, in *RawGetRequest, opts ...grpc.CallOption) (*RawGetResponse, error)
}

type icecaneKVClient struct {
	cc grpc.ClientConnInterface
}

func NewIcecaneKVClient(cc grpc.ClientConnInterface) IcecaneKVClient {
	return &icecaneKVClient{cc}
}

func (c *icecaneKVClient) RawGet(ctx context.Context, in *RawGetRequest, opts ...grpc.CallOption) (*RawGetResponse, error) {
	out := new(RawGetResponse)
	err := c.cc.Invoke(ctx, "/icecanedbpb.IcecaneKV/RawGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// IcecaneKVServer is the server API for IcecaneKV service.
// All implementations must embed UnimplementedIcecaneKVServer
// for forward compatibility
type IcecaneKVServer interface {
	// RawKV commands.
	RawGet(context.Context, *RawGetRequest) (*RawGetResponse, error)
	mustEmbedUnimplementedIcecaneKVServer()
}

// UnimplementedIcecaneKVServer must be embedded to have forward compatible implementations.
type UnimplementedIcecaneKVServer struct {
}

func (UnimplementedIcecaneKVServer) RawGet(context.Context, *RawGetRequest) (*RawGetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RawGet not implemented")
}
func (UnimplementedIcecaneKVServer) mustEmbedUnimplementedIcecaneKVServer() {}

// UnsafeIcecaneKVServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to IcecaneKVServer will
// result in compilation errors.
type UnsafeIcecaneKVServer interface {
	mustEmbedUnimplementedIcecaneKVServer()
}

func RegisterIcecaneKVServer(s grpc.ServiceRegistrar, srv IcecaneKVServer) {
	s.RegisterService(&_IcecaneKV_serviceDesc, srv)
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

var _IcecaneKV_serviceDesc = grpc.ServiceDesc{
	ServiceName: "icecanedbpb.IcecaneKV",
	HandlerType: (*IcecaneKVServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RawGet",
			Handler:    _IcecaneKV_RawGet_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "icecanekvpb.proto",
}
