// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0-devel
// 	protoc        v3.12.1
// source: icecanekvpb.proto

package icecanedbpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_icecanekvpb_proto protoreflect.FileDescriptor

var file_icecanekvpb_proto_rawDesc = []byte{
	0x0a, 0x11, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x6b, 0x76, 0x70, 0x62, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62,
	0x1a, 0x0d, 0x6b, 0x76, 0x72, 0x70, 0x63, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x0c, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x32, 0xb7, 0x02,
	0x0a, 0x09, 0x49, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x4b, 0x56, 0x12, 0x43, 0x0a, 0x06, 0x52,
	0x61, 0x77, 0x47, 0x65, 0x74, 0x12, 0x1a, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64,
	0x62, 0x70, 0x62, 0x2e, 0x52, 0x61, 0x77, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1b, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e,
	0x52, 0x61, 0x77, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00,
	0x12, 0x43, 0x0a, 0x06, 0x52, 0x61, 0x77, 0x50, 0x75, 0x74, 0x12, 0x1a, 0x2e, 0x69, 0x63, 0x65,
	0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x52, 0x61, 0x77, 0x50, 0x75, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1b, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65,
	0x64, 0x62, 0x70, 0x62, 0x2e, 0x52, 0x61, 0x77, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x4c, 0x0a, 0x09, 0x52, 0x61, 0x77, 0x44, 0x65, 0x6c, 0x65,
	0x74, 0x65, 0x12, 0x1d, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62,
	0x2e, 0x52, 0x61, 0x77, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1e, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e,
	0x52, 0x61, 0x77, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x00, 0x12, 0x52, 0x0a, 0x0b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f,
	0x74, 0x65, 0x12, 0x1f, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62,
	0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70,
	0x62, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var file_icecanekvpb_proto_goTypes = []interface{}{
	(*RawGetRequest)(nil),       // 0: icecanedbpb.RawGetRequest
	(*RawPutRequest)(nil),       // 1: icecanedbpb.RawPutRequest
	(*RawDeleteRequest)(nil),    // 2: icecanedbpb.RawDeleteRequest
	(*RequestVoteRequest)(nil),  // 3: icecanedbpb.RequestVoteRequest
	(*RawGetResponse)(nil),      // 4: icecanedbpb.RawGetResponse
	(*RawPutResponse)(nil),      // 5: icecanedbpb.RawPutResponse
	(*RawDeleteResponse)(nil),   // 6: icecanedbpb.RawDeleteResponse
	(*RequestVoteResponse)(nil), // 7: icecanedbpb.RequestVoteResponse
}
var file_icecanekvpb_proto_depIdxs = []int32{
	0, // 0: icecanedbpb.IcecaneKV.RawGet:input_type -> icecanedbpb.RawGetRequest
	1, // 1: icecanedbpb.IcecaneKV.RawPut:input_type -> icecanedbpb.RawPutRequest
	2, // 2: icecanedbpb.IcecaneKV.RawDelete:input_type -> icecanedbpb.RawDeleteRequest
	3, // 3: icecanedbpb.IcecaneKV.RequestVote:input_type -> icecanedbpb.RequestVoteRequest
	4, // 4: icecanedbpb.IcecaneKV.RawGet:output_type -> icecanedbpb.RawGetResponse
	5, // 5: icecanedbpb.IcecaneKV.RawPut:output_type -> icecanedbpb.RawPutResponse
	6, // 6: icecanedbpb.IcecaneKV.RawDelete:output_type -> icecanedbpb.RawDeleteResponse
	7, // 7: icecanedbpb.IcecaneKV.RequestVote:output_type -> icecanedbpb.RequestVoteResponse
	4, // [4:8] is the sub-list for method output_type
	0, // [0:4] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_icecanekvpb_proto_init() }
func file_icecanekvpb_proto_init() {
	if File_icecanekvpb_proto != nil {
		return
	}
	file_kvrpcpb_proto_init()
	file_raftpb_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_icecanekvpb_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_icecanekvpb_proto_goTypes,
		DependencyIndexes: file_icecanekvpb_proto_depIdxs,
	}.Build()
	File_icecanekvpb_proto = out.File
	file_icecanekvpb_proto_rawDesc = nil
	file_icecanekvpb_proto_goTypes = nil
	file_icecanekvpb_proto_depIdxs = nil
}
