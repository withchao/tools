package inprocess

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/proto"
)

type messageCodec interface {
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}

func newProtoCodec() messageCodec {
	return protoCodec{}
}

// protoCodec marshals proto messages, and passes raw bytes through so the
// api RpcInvoke endpoint can relay pre-marshaled requests/responses.
type protoCodec struct{}

func (protoCodec) Marshal(in any) ([]byte, error) {
	switch v := in.(type) {
	case proto.Message:
		return proto.Marshal(v)
	case []byte:
		return v, nil
	case *[]byte:
		return *v, nil
	default:
		return nil, fmt.Errorf("inprocess message codec marshal unsupported type %T", in)
	}
}

func (protoCodec) Unmarshal(b []byte, out any) error {
	switch v := out.(type) {
	case proto.Message:
		return proto.Unmarshal(b, v)
	case *[]byte:
		*v = bytes.Clone(b)
		return nil
	default:
		return fmt.Errorf("inprocess message codec unmarshal unsupported type %T", out)
	}
}
