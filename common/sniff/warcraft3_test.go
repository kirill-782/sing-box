package sniff_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/sniff"
	C "github.com/sagernet/sing-box/constant"

	"github.com/stretchr/testify/require"
)

func TestSniffWarcraft3W3GS(t *testing.T) {
	t.Parallel()

	var metadata adapter.InboundContext
	err := sniff.Warcraft3(context.TODO(), &metadata, bytes.NewReader([]byte{0xF7, 0x1E, 0x04, 0x00}))
	require.NoError(t, err)
	require.Equal(t, C.ProtocolWarcraft3, metadata.Protocol)
}

func TestSniffWarcraft3GPS(t *testing.T) {
	t.Parallel()

	var metadata adapter.InboundContext
	err := sniff.Warcraft3(context.TODO(), &metadata, bytes.NewReader([]byte{0xF8, 0x02, 0x05, 0x00, 0x00}))
	require.NoError(t, err)
	require.Equal(t, C.ProtocolWarcraft3, metadata.Protocol)
}

func TestSniffIncompleteWarcraft3(t *testing.T) {
	t.Parallel()

	var metadata adapter.InboundContext
	err := sniff.Warcraft3(context.TODO(), &metadata, bytes.NewReader([]byte{0xF8, 0x0A, 0x05, 0x00}))
	require.ErrorIs(t, err, sniff.ErrNeedMoreData)
}

func TestSniffNotWarcraft3(t *testing.T) {
	t.Parallel()

	var metadata adapter.InboundContext
	err := sniff.Warcraft3(context.TODO(), &metadata, bytes.NewReader([]byte{0xF7, 0x1F, 0x04, 0x00}))
	require.ErrorIs(t, err, os.ErrInvalid)
}
