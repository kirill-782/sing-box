package option

import (
	"context"
	"testing"

	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

func TestDirectOutboundProxyProtocolUnmarshalJSON(t *testing.T) {
	t.Parallel()

	var options DirectOutboundOptions
	err := json.UnmarshalContext(context.Background(), []byte(`{"proxy_protocol":2}`), &options)
	require.NoError(t, err)
	require.Equal(t, uint8(2), options.ProxyProtocol)
}

func TestDirectOutboundProxyProtocolCompatUnmarshalJSON(t *testing.T) {
	t.Parallel()

	var options DirectOutboundOptions
	err := json.UnmarshalContext(context.Background(), []byte(`{"proxyProtocol":1}`), &options)
	require.NoError(t, err)
	require.Equal(t, uint8(1), options.ProxyProtocol)
}

func TestDirectOutboundProxyProtocolRejectsInvalidVersion(t *testing.T) {
	t.Parallel()

	var options DirectOutboundOptions
	err := json.UnmarshalContext(context.Background(), []byte(`{"proxy_protocol":3}`), &options)
	require.EqualError(t, err, "invalid proxy protocol version: 3")
}

func TestDirectOutboundProxyProtocolRejectsConflict(t *testing.T) {
	t.Parallel()

	var options DirectOutboundOptions
	err := json.UnmarshalContext(context.Background(), []byte(`{"proxy_protocol":1,"proxyProtocol":2}`), &options)
	require.EqualError(t, err, "conflicting proxy protocol options")
}
