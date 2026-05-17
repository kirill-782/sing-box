package direct

import (
	"bytes"
	"encoding/hex"
	"testing"

	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

func TestWriteProxyProtocolV1(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer
	err := writeProxyProtocolHeaderTo(&buffer, 1, M.ParseSocksaddr("192.0.2.1:12345"), M.ParseSocksaddr("198.51.100.2:443"))
	require.NoError(t, err)
	require.Equal(t, "PROXY TCP4 192.0.2.1 198.51.100.2 12345 443\r\n", buffer.String())
}

func TestWriteProxyProtocolV1Unknown(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer
	err := writeProxyProtocolHeaderTo(&buffer, 1, M.Socksaddr{}, M.ParseSocksaddr("198.51.100.2:443"))
	require.NoError(t, err)
	require.Equal(t, "PROXY UNKNOWN\r\n", buffer.String())
}

func TestWriteProxyProtocolV2(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer
	err := writeProxyProtocolHeaderTo(&buffer, 2, M.ParseSocksaddr("192.0.2.1:12345"), M.ParseSocksaddr("198.51.100.2:443"))
	require.NoError(t, err)
	expected, err := hex.DecodeString("0d0a0d0a000d0a515549540a2111000cc0000201c6336402303901bb")
	require.NoError(t, err)
	require.Equal(t, expected, buffer.Bytes())
}

func TestWriteProxyProtocolV2Unknown(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer
	err := writeProxyProtocolHeaderTo(&buffer, 2, M.Socksaddr{}, M.ParseSocksaddr("198.51.100.2:443"))
	require.NoError(t, err)
	expected, err := hex.DecodeString("0d0a0d0a000d0a515549540a20000000")
	require.NoError(t, err)
	require.Equal(t, expected, buffer.Bytes())
}

func TestWriteProxyProtocolRejectsInvalidVersion(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer
	err := writeProxyProtocolHeaderTo(&buffer, 3, M.ParseSocksaddr("192.0.2.1:12345"), M.ParseSocksaddr("198.51.100.2:443"))
	require.EqualError(t, err, "invalid proxy protocol version: 3")
}
