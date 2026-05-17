package sniff

import (
	"context"
	"encoding/binary"
	"io"
	"os"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/rw"
)

const (
	warcraft3HeaderSize = 4

	warcraft3W3GSMarker = 0xF7
	warcraft3GPSMarker  = 0xF8

	warcraft3W3GSInit = 0x1E
	warcraft3GPSInit  = 0x02
	warcraft3GPSAck   = 0x0A
)

func Warcraft3(_ context.Context, metadata *adapter.InboundContext, reader io.Reader) error {
	var header [warcraft3HeaderSize]byte
	_, err := io.ReadFull(reader, header[:])
	if err != nil {
		return E.Cause1(ErrNeedMoreData, err)
	}
	total, err := parseWarcraft3Header(header[:])
	if err != nil {
		return err
	}
	if total > warcraft3HeaderSize {
		err = rw.SkipN(reader, total-warcraft3HeaderSize)
		if err != nil {
			return E.Cause1(ErrNeedMoreData, err)
		}
	}
	metadata.Protocol = C.ProtocolWarcraft3
	return nil
}

func parseWarcraft3Header(header []byte) (int, error) {
	switch header[0] {
	case warcraft3W3GSMarker:
		if header[1] != warcraft3W3GSInit {
			return 0, os.ErrInvalid
		}
	case warcraft3GPSMarker:
		if header[1] != warcraft3GPSInit && header[1] != warcraft3GPSAck {
			return 0, os.ErrInvalid
		}
	default:
		return 0, os.ErrInvalid
	}
	total := int(binary.LittleEndian.Uint16(header[2:4]))
	if total < warcraft3HeaderSize {
		return 0, os.ErrInvalid
	}
	return total, nil
}
