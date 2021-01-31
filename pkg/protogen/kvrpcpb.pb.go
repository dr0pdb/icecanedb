// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0-devel
// 	protoc        v3.12.1
// source: kvrpcpb.proto

package icecanedbpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RawGetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Context *Context `protobuf:"bytes,1,opt,name=context,proto3" json:"context,omitempty"`
	Key     []byte   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *RawGetRequest) Reset() {
	*x = RawGetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvrpcpb_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RawGetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RawGetRequest) ProtoMessage() {}

func (x *RawGetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kvrpcpb_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RawGetRequest.ProtoReflect.Descriptor instead.
func (*RawGetRequest) Descriptor() ([]byte, []int) {
	return file_kvrpcpb_proto_rawDescGZIP(), []int{0}
}

func (x *RawGetRequest) GetContext() *Context {
	if x != nil {
		return x.Context
	}
	return nil
}

func (x *RawGetRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

type RawGetResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *RawGetResponse) Reset() {
	*x = RawGetResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvrpcpb_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RawGetResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RawGetResponse) ProtoMessage() {}

func (x *RawGetResponse) ProtoReflect() protoreflect.Message {
	mi := &file_kvrpcpb_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RawGetResponse.ProtoReflect.Descriptor instead.
func (*RawGetResponse) Descriptor() ([]byte, []int) {
	return file_kvrpcpb_proto_rawDescGZIP(), []int{1}
}

// Context is the metadata that is present in every request.
type Context struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Context) Reset() {
	*x = Context{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvrpcpb_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Context) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Context) ProtoMessage() {}

func (x *Context) ProtoReflect() protoreflect.Message {
	mi := &file_kvrpcpb_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Context.ProtoReflect.Descriptor instead.
func (*Context) Descriptor() ([]byte, []int) {
	return file_kvrpcpb_proto_rawDescGZIP(), []int{2}
}

var File_kvrpcpb_proto protoreflect.FileDescriptor

var file_kvrpcpb_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x6b, 0x76, 0x72, 0x70, 0x63, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x0b, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x22, 0x51, 0x0a, 0x0d,
	0x52, 0x61, 0x77, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2e, 0x0a,
	0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14,
	0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x43, 0x6f, 0x6e,
	0x74, 0x65, 0x78, 0x74, 0x52, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x12, 0x10, 0x0a,
	0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x22,
	0x10, 0x0a, 0x0e, 0x52, 0x61, 0x77, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x09, 0x0a, 0x07, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_kvrpcpb_proto_rawDescOnce sync.Once
	file_kvrpcpb_proto_rawDescData = file_kvrpcpb_proto_rawDesc
)

func file_kvrpcpb_proto_rawDescGZIP() []byte {
	file_kvrpcpb_proto_rawDescOnce.Do(func() {
		file_kvrpcpb_proto_rawDescData = protoimpl.X.CompressGZIP(file_kvrpcpb_proto_rawDescData)
	})
	return file_kvrpcpb_proto_rawDescData
}

var file_kvrpcpb_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_kvrpcpb_proto_goTypes = []interface{}{
	(*RawGetRequest)(nil),  // 0: icecanedbpb.RawGetRequest
	(*RawGetResponse)(nil), // 1: icecanedbpb.RawGetResponse
	(*Context)(nil),        // 2: icecanedbpb.Context
}
var file_kvrpcpb_proto_depIdxs = []int32{
	2, // 0: icecanedbpb.RawGetRequest.context:type_name -> icecanedbpb.Context
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_kvrpcpb_proto_init() }
func file_kvrpcpb_proto_init() {
	if File_kvrpcpb_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_kvrpcpb_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RawGetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvrpcpb_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RawGetResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvrpcpb_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Context); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_kvrpcpb_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_kvrpcpb_proto_goTypes,
		DependencyIndexes: file_kvrpcpb_proto_depIdxs,
		MessageInfos:      file_kvrpcpb_proto_msgTypes,
	}.Build()
	File_kvrpcpb_proto = out.File
	file_kvrpcpb_proto_rawDesc = nil
	file_kvrpcpb_proto_goTypes = nil
	file_kvrpcpb_proto_depIdxs = nil
}
