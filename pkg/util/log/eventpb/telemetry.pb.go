// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: util/log/eventpb/telemetry.proto

package eventpb

import (
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

// SampledQuery
type SampledQuery struct {
	CommonEventDetails    `protobuf:"bytes,1,opt,name=common,proto3,embedded=common" json:""`
	CommonSQLEventDetails `protobuf:"bytes,2,opt,name=sql,proto3,embedded=sql" json:""`
	CommonSQLExecDetails  `protobuf:"bytes,3,opt,name=exec,proto3,embedded=exec" json:""`
}

func (m *SampledQuery) Reset()         { *m = SampledQuery{} }
func (m *SampledQuery) String() string { return proto.CompactTextString(m) }
func (*SampledQuery) ProtoMessage()    {}
func (*SampledQuery) Descriptor() ([]byte, []int) {
	return fileDescriptor_3d317b4ad74be4f7, []int{0}
}
func (m *SampledQuery) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SampledQuery) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	b = b[:cap(b)]
	n, err := m.MarshalToSizedBuffer(b)
	if err != nil {
		return nil, err
	}
	return b[:n], nil
}
func (m *SampledQuery) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SampledQuery.Merge(m, src)
}
func (m *SampledQuery) XXX_Size() int {
	return m.Size()
}
func (m *SampledQuery) XXX_DiscardUnknown() {
	xxx_messageInfo_SampledQuery.DiscardUnknown(m)
}

var xxx_messageInfo_SampledQuery proto.InternalMessageInfo

func init() {
	proto.RegisterType((*SampledQuery)(nil), "cockroach.util.log.eventpb.SampledQuery")
}

func init() { proto.RegisterFile("util/log/eventpb/telemetry.proto", fileDescriptor_3d317b4ad74be4f7) }

var fileDescriptor_3d317b4ad74be4f7 = []byte{
	// 276 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x52, 0x28, 0x2d, 0xc9, 0xcc,
	0xd1, 0xcf, 0xc9, 0x4f, 0xd7, 0x4f, 0x2d, 0x4b, 0xcd, 0x2b, 0x29, 0x48, 0xd2, 0x2f, 0x49, 0xcd,
	0x49, 0xcd, 0x4d, 0x2d, 0x29, 0xaa, 0xd4, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x92, 0x4a, 0xce,
	0x4f, 0xce, 0x2e, 0xca, 0x4f, 0x4c, 0xce, 0xd0, 0x03, 0xa9, 0xd5, 0xcb, 0xc9, 0x4f, 0xd7, 0x83,
	0xaa, 0x95, 0x12, 0x49, 0xcf, 0x4f, 0xcf, 0x07, 0x2b, 0xd3, 0x07, 0xb1, 0x20, 0x3a, 0xa4, 0x64,
	0x31, 0xcc, 0x04, 0xd3, 0xc5, 0x50, 0x69, 0x75, 0x0c, 0xe9, 0xe2, 0xc2, 0x9c, 0xf8, 0xc4, 0xd2,
	0x94, 0xcc, 0x92, 0x78, 0x64, 0x85, 0x4a, 0xdd, 0x4c, 0x5c, 0x3c, 0xc1, 0x89, 0xb9, 0x05, 0x39,
	0xa9, 0x29, 0x81, 0xa5, 0xa9, 0x45, 0x95, 0x42, 0x21, 0x5c, 0x6c, 0xc9, 0xf9, 0xb9, 0xb9, 0xf9,
	0x79, 0x12, 0x8c, 0x0a, 0x8c, 0x1a, 0xdc, 0x46, 0x7a, 0x7a, 0xb8, 0xdd, 0xa6, 0xe7, 0x0c, 0x56,
	0xe9, 0x0a, 0xe2, 0xb9, 0xa4, 0x96, 0x24, 0x66, 0xe6, 0x14, 0x3b, 0xf1, 0x9c, 0xb8, 0x27, 0xcf,
	0x70, 0xe1, 0x9e, 0x3c, 0xe3, 0xab, 0x7b, 0xf2, 0x0c, 0x41, 0x50, 0xb3, 0x84, 0x02, 0xb9, 0x98,
	0x8b, 0x0b, 0x73, 0x24, 0x98, 0xc0, 0x46, 0x1a, 0x12, 0x36, 0x32, 0x38, 0xd0, 0x07, 0x8f, 0xa9,
	0x20, 0xb3, 0x84, 0x82, 0xb8, 0x58, 0x52, 0x2b, 0x52, 0x93, 0x25, 0x98, 0xc1, 0x66, 0x1a, 0x10,
	0x67, 0x66, 0x45, 0x6a, 0x32, 0x76, 0x23, 0xc1, 0x66, 0x39, 0x69, 0x9e, 0x78, 0x28, 0xc7, 0x70,
	0xe2, 0x91, 0x1c, 0xe3, 0x85, 0x47, 0x72, 0x8c, 0x37, 0x1e, 0xc9, 0x31, 0x3e, 0x78, 0x24, 0xc7,
	0x38, 0xe1, 0xb1, 0x1c, 0xc3, 0x85, 0xc7, 0x72, 0x0c, 0x37, 0x1e, 0xcb, 0x31, 0x44, 0xb1, 0x43,
	0xcd, 0x4c, 0x62, 0x03, 0x87, 0x9f, 0x31, 0x20, 0x00, 0x00, 0xff, 0xff, 0x20, 0xc5, 0x6a, 0x31,
	0xdd, 0x01, 0x00, 0x00,
}

func (m *SampledQuery) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SampledQuery) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SampledQuery) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	{
		size, err := m.CommonSQLExecDetails.MarshalToSizedBuffer(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarintTelemetry(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0x1a
	{
		size, err := m.CommonSQLEventDetails.MarshalToSizedBuffer(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarintTelemetry(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0x12
	{
		size, err := m.CommonEventDetails.MarshalToSizedBuffer(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarintTelemetry(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0xa
	return len(dAtA) - i, nil
}

func encodeVarintTelemetry(dAtA []byte, offset int, v uint64) int {
	offset -= sovTelemetry(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *SampledQuery) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = m.CommonEventDetails.Size()
	n += 1 + l + sovTelemetry(uint64(l))
	l = m.CommonSQLEventDetails.Size()
	n += 1 + l + sovTelemetry(uint64(l))
	l = m.CommonSQLExecDetails.Size()
	n += 1 + l + sovTelemetry(uint64(l))
	return n
}

func sovTelemetry(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTelemetry(x uint64) (n int) {
	return sovTelemetry(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *SampledQuery) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTelemetry
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: SampledQuery: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SampledQuery: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CommonEventDetails", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTelemetry
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTelemetry
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTelemetry
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.CommonEventDetails.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CommonSQLEventDetails", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTelemetry
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTelemetry
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTelemetry
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.CommonSQLEventDetails.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CommonSQLExecDetails", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTelemetry
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTelemetry
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTelemetry
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.CommonSQLExecDetails.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTelemetry(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTelemetry
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipTelemetry(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTelemetry
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTelemetry
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTelemetry
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTelemetry
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTelemetry
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTelemetry
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthTelemetry        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTelemetry          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTelemetry = fmt.Errorf("proto: unexpected end of group")
)
