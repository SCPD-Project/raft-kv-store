// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.22.0-devel
// 	protoc        v3.11.4
// source: raftpb/raft.proto

package raftpb

import (
	proto "github.com/golang/protobuf/proto"
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

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type Command struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Method string `protobuf:"bytes,1,opt,name=method,proto3" json:"method,omitempty"`
	Key    string `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Value  string `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Command) Reset() {
	*x = Command{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_raft_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Command) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Command) ProtoMessage() {}

func (x *Command) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_raft_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Command.ProtoReflect.Descriptor instead.
func (*Command) Descriptor() ([]byte, []int) {
	return file_raftpb_raft_proto_rawDescGZIP(), []int{0}
}

func (x *Command) GetMethod() string {
	if x != nil {
		return x.Method
	}
	return ""
}

func (x *Command) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Command) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

type RaftCommand struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Commands []*Command `protobuf:"bytes,1,rep,name=commands,proto3" json:"commands,omitempty"`
}

func (x *RaftCommand) Reset() {
	*x = RaftCommand{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_raft_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RaftCommand) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RaftCommand) ProtoMessage() {}

func (x *RaftCommand) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_raft_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RaftCommand.ProtoReflect.Descriptor instead.
func (*RaftCommand) Descriptor() ([]byte, []int) {
	return file_raftpb_raft_proto_rawDescGZIP(), []int{1}
}

func (x *RaftCommand) GetCommands() []*Command {
	if x != nil {
		return x.Commands
	}
	return nil
}

type JoinMsg struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	RaftAddress string `protobuf:"bytes,1,opt,name=RaftAddress,proto3" json:"RaftAddress,omitempty"`
	ID          string `protobuf:"bytes,2,opt,name=ID,proto3" json:"ID,omitempty"`
}

func (x *JoinMsg) Reset() {
	*x = JoinMsg{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_raft_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JoinMsg) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JoinMsg) ProtoMessage() {}

func (x *JoinMsg) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_raft_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JoinMsg.ProtoReflect.Descriptor instead.
func (*JoinMsg) Descriptor() ([]byte, []int) {
	return file_raftpb_raft_proto_rawDescGZIP(), []int{2}
}

func (x *JoinMsg) GetRaftAddress() string {
	if x != nil {
		return x.RaftAddress
	}
	return ""
}

func (x *JoinMsg) GetID() string {
	if x != nil {
		return x.ID
	}
	return ""
}

type RegisterMsg struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ShardID string `protobuf:"bytes,1,opt,name=ShardID,proto3" json:"ShardID,omitempty"`
	Address string `protobuf:"bytes,2,opt,name=Address,proto3" json:"Address,omitempty"`
}

func (x *RegisterMsg) Reset() {
	*x = RegisterMsg{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raftpb_raft_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RegisterMsg) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterMsg) ProtoMessage() {}

func (x *RegisterMsg) ProtoReflect() protoreflect.Message {
	mi := &file_raftpb_raft_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterMsg.ProtoReflect.Descriptor instead.
func (*RegisterMsg) Descriptor() ([]byte, []int) {
	return file_raftpb_raft_proto_rawDescGZIP(), []int{3}
}

func (x *RegisterMsg) GetShardID() string {
	if x != nil {
		return x.ShardID
	}
	return ""
}

func (x *RegisterMsg) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

var File_raftpb_raft_proto protoreflect.FileDescriptor

var file_raftpb_raft_proto_rawDesc = []byte{
	0x0a, 0x11, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2f, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x06, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x22, 0x49, 0x0a, 0x07, 0x43,
	0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x12, 0x10,
	0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3a, 0x0a, 0x0b, 0x52, 0x61, 0x66, 0x74, 0x43, 0x6f,
	0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x12, 0x2b, 0x0a, 0x08, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64,
	0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62,
	0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52, 0x08, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e,
	0x64, 0x73, 0x22, 0x3b, 0x0a, 0x07, 0x4a, 0x6f, 0x69, 0x6e, 0x4d, 0x73, 0x67, 0x12, 0x20, 0x0a,
	0x0b, 0x52, 0x61, 0x66, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0b, 0x52, 0x61, 0x66, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x12,
	0x0e, 0x0a, 0x02, 0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x49, 0x44, 0x22,
	0x41, 0x0a, 0x0b, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x4d, 0x73, 0x67, 0x12, 0x18,
	0x0a, 0x07, 0x53, 0x68, 0x61, 0x72, 0x64, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x07, 0x53, 0x68, 0x61, 0x72, 0x64, 0x49, 0x44, 0x12, 0x18, 0x0a, 0x07, 0x41, 0x64, 0x64, 0x72,
	0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x41, 0x64, 0x64, 0x72, 0x65,
	0x73, 0x73, 0x42, 0x08, 0x5a, 0x06, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_raftpb_raft_proto_rawDescOnce sync.Once
	file_raftpb_raft_proto_rawDescData = file_raftpb_raft_proto_rawDesc
)

func file_raftpb_raft_proto_rawDescGZIP() []byte {
	file_raftpb_raft_proto_rawDescOnce.Do(func() {
		file_raftpb_raft_proto_rawDescData = protoimpl.X.CompressGZIP(file_raftpb_raft_proto_rawDescData)
	})
	return file_raftpb_raft_proto_rawDescData
}

var file_raftpb_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_raftpb_raft_proto_goTypes = []interface{}{
	(*Command)(nil),     // 0: raftpb.Command
	(*RaftCommand)(nil), // 1: raftpb.RaftCommand
	(*JoinMsg)(nil),     // 2: raftpb.JoinMsg
	(*RegisterMsg)(nil), // 3: raftpb.RegisterMsg
}
var file_raftpb_raft_proto_depIdxs = []int32{
	0, // 0: raftpb.RaftCommand.commands:type_name -> raftpb.Command
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_raftpb_raft_proto_init() }
func file_raftpb_raft_proto_init() {
	if File_raftpb_raft_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_raftpb_raft_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Command); i {
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
		file_raftpb_raft_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RaftCommand); i {
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
		file_raftpb_raft_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JoinMsg); i {
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
		file_raftpb_raft_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RegisterMsg); i {
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
			RawDescriptor: file_raftpb_raft_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_raftpb_raft_proto_goTypes,
		DependencyIndexes: file_raftpb_raft_proto_depIdxs,
		MessageInfos:      file_raftpb_raft_proto_msgTypes,
	}.Build()
	File_raftpb_raft_proto = out.File
	file_raftpb_raft_proto_rawDesc = nil
	file_raftpb_raft_proto_goTypes = nil
	file_raftpb_raft_proto_depIdxs = nil
}
