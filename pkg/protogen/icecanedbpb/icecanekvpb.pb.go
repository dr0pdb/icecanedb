// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.15.8
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
	0x0c, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x32, 0xbb, 0x06,
	0x0a, 0x09, 0x49, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x4b, 0x56, 0x12, 0x3a, 0x0a, 0x03, 0x47,
	0x65, 0x74, 0x12, 0x17, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62,
	0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x18, 0x2e, 0x69, 0x63,
	0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x3d, 0x0a, 0x04, 0x53, 0x63, 0x61, 0x6e, 0x12,
	0x18, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x53, 0x63,
	0x61, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x69, 0x63, 0x65, 0x63,
	0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x53, 0x63, 0x61, 0x6e, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x3a, 0x0a, 0x03, 0x53, 0x65, 0x74, 0x12, 0x17, 0x2e,
	0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x53, 0x65, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x18, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65,
	0x64, 0x62, 0x70, 0x62, 0x2e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x00, 0x12, 0x43, 0x0a, 0x06, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x1a, 0x2e, 0x69,
	0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1b, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61,
	0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x49, 0x0a, 0x08, 0x42, 0x65, 0x67, 0x69, 0x6e,
	0x54, 0x78, 0x6e, 0x12, 0x1c, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70,
	0x62, 0x2e, 0x42, 0x65, 0x67, 0x69, 0x6e, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1d, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e,
	0x42, 0x65, 0x67, 0x69, 0x6e, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x00, 0x12, 0x4c, 0x0a, 0x09, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x78, 0x6e, 0x12,
	0x1d, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x43, 0x6f,
	0x6d, 0x6d, 0x69, 0x74, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e,
	0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x43, 0x6f, 0x6d,
	0x6d, 0x69, 0x74, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00,
	0x12, 0x52, 0x0a, 0x0b, 0x52, 0x6f, 0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b, 0x54, 0x78, 0x6e, 0x12,
	0x1f, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x52, 0x6f,
	0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x20, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x52,
	0x6f, 0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x00, 0x12, 0x52, 0x0a, 0x0b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56,
	0x6f, 0x74, 0x65, 0x12, 0x1f, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70,
	0x62, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62,
	0x70, 0x62, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x58, 0x0a, 0x0d, 0x41, 0x70, 0x70, 0x65,
	0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x21, 0x2e, 0x69, 0x63, 0x65, 0x63,
	0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e,
	0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x22, 0x2e, 0x69,
	0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e,
	0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x00, 0x12, 0x46, 0x0a, 0x07, 0x50, 0x65, 0x65, 0x72, 0x53, 0x65, 0x74, 0x12, 0x1b, 0x2e,
	0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x50, 0x65, 0x65, 0x72,
	0x53, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1c, 0x2e, 0x69, 0x63, 0x65,
	0x63, 0x61, 0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x53, 0x65, 0x74,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x4f, 0x0a, 0x0a, 0x50, 0x65,
	0x65, 0x72, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x1e, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61,
	0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x44, 0x65, 0x6c, 0x65, 0x74,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x69, 0x63, 0x65, 0x63, 0x61,
	0x6e, 0x65, 0x64, 0x62, 0x70, 0x62, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x44, 0x65, 0x6c, 0x65, 0x74,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x16, 0x5a, 0x14, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x67, 0x65, 0x6e, 0x2f, 0x69, 0x63, 0x65, 0x63, 0x61, 0x6e, 0x65, 0x64,
	0x62, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var file_icecanekvpb_proto_goTypes = []interface{}{
	(*GetRequest)(nil),            // 0: icecanedbpb.GetRequest
	(*ScanRequest)(nil),           // 1: icecanedbpb.ScanRequest
	(*SetRequest)(nil),            // 2: icecanedbpb.SetRequest
	(*DeleteRequest)(nil),         // 3: icecanedbpb.DeleteRequest
	(*BeginTxnRequest)(nil),       // 4: icecanedbpb.BeginTxnRequest
	(*CommitTxnRequest)(nil),      // 5: icecanedbpb.CommitTxnRequest
	(*RollbackTxnRequest)(nil),    // 6: icecanedbpb.RollbackTxnRequest
	(*RequestVoteRequest)(nil),    // 7: icecanedbpb.RequestVoteRequest
	(*AppendEntriesRequest)(nil),  // 8: icecanedbpb.AppendEntriesRequest
	(*PeerSetRequest)(nil),        // 9: icecanedbpb.PeerSetRequest
	(*PeerDeleteRequest)(nil),     // 10: icecanedbpb.PeerDeleteRequest
	(*GetResponse)(nil),           // 11: icecanedbpb.GetResponse
	(*ScanResponse)(nil),          // 12: icecanedbpb.ScanResponse
	(*SetResponse)(nil),           // 13: icecanedbpb.SetResponse
	(*DeleteResponse)(nil),        // 14: icecanedbpb.DeleteResponse
	(*BeginTxnResponse)(nil),      // 15: icecanedbpb.BeginTxnResponse
	(*CommitTxnResponse)(nil),     // 16: icecanedbpb.CommitTxnResponse
	(*RollbackTxnResponse)(nil),   // 17: icecanedbpb.RollbackTxnResponse
	(*RequestVoteResponse)(nil),   // 18: icecanedbpb.RequestVoteResponse
	(*AppendEntriesResponse)(nil), // 19: icecanedbpb.AppendEntriesResponse
	(*PeerSetResponse)(nil),       // 20: icecanedbpb.PeerSetResponse
	(*PeerDeleteResponse)(nil),    // 21: icecanedbpb.PeerDeleteResponse
}
var file_icecanekvpb_proto_depIdxs = []int32{
	0,  // 0: icecanedbpb.IcecaneKV.Get:input_type -> icecanedbpb.GetRequest
	1,  // 1: icecanedbpb.IcecaneKV.Scan:input_type -> icecanedbpb.ScanRequest
	2,  // 2: icecanedbpb.IcecaneKV.Set:input_type -> icecanedbpb.SetRequest
	3,  // 3: icecanedbpb.IcecaneKV.Delete:input_type -> icecanedbpb.DeleteRequest
	4,  // 4: icecanedbpb.IcecaneKV.BeginTxn:input_type -> icecanedbpb.BeginTxnRequest
	5,  // 5: icecanedbpb.IcecaneKV.CommitTxn:input_type -> icecanedbpb.CommitTxnRequest
	6,  // 6: icecanedbpb.IcecaneKV.RollbackTxn:input_type -> icecanedbpb.RollbackTxnRequest
	7,  // 7: icecanedbpb.IcecaneKV.RequestVote:input_type -> icecanedbpb.RequestVoteRequest
	8,  // 8: icecanedbpb.IcecaneKV.AppendEntries:input_type -> icecanedbpb.AppendEntriesRequest
	9,  // 9: icecanedbpb.IcecaneKV.PeerSet:input_type -> icecanedbpb.PeerSetRequest
	10, // 10: icecanedbpb.IcecaneKV.PeerDelete:input_type -> icecanedbpb.PeerDeleteRequest
	11, // 11: icecanedbpb.IcecaneKV.Get:output_type -> icecanedbpb.GetResponse
	12, // 12: icecanedbpb.IcecaneKV.Scan:output_type -> icecanedbpb.ScanResponse
	13, // 13: icecanedbpb.IcecaneKV.Set:output_type -> icecanedbpb.SetResponse
	14, // 14: icecanedbpb.IcecaneKV.Delete:output_type -> icecanedbpb.DeleteResponse
	15, // 15: icecanedbpb.IcecaneKV.BeginTxn:output_type -> icecanedbpb.BeginTxnResponse
	16, // 16: icecanedbpb.IcecaneKV.CommitTxn:output_type -> icecanedbpb.CommitTxnResponse
	17, // 17: icecanedbpb.IcecaneKV.RollbackTxn:output_type -> icecanedbpb.RollbackTxnResponse
	18, // 18: icecanedbpb.IcecaneKV.RequestVote:output_type -> icecanedbpb.RequestVoteResponse
	19, // 19: icecanedbpb.IcecaneKV.AppendEntries:output_type -> icecanedbpb.AppendEntriesResponse
	20, // 20: icecanedbpb.IcecaneKV.PeerSet:output_type -> icecanedbpb.PeerSetResponse
	21, // 21: icecanedbpb.IcecaneKV.PeerDelete:output_type -> icecanedbpb.PeerDeleteResponse
	11, // [11:22] is the sub-list for method output_type
	0,  // [0:11] is the sub-list for method input_type
	0,  // [0:0] is the sub-list for extension type_name
	0,  // [0:0] is the sub-list for extension extendee
	0,  // [0:0] is the sub-list for field type_name
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
