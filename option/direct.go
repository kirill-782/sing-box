package option

import (
	"context"

	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json"
)

type DirectInboundOptions struct {
	ListenOptions
	Network         NetworkList `json:"network,omitempty"`
	OverrideAddress string      `json:"override_address,omitempty"`
	OverridePort    uint16      `json:"override_port,omitempty"`
}

type _DirectOutboundOptions struct {
	DialerOptions
	// Deprecated: Use Route Action instead
	OverrideAddress string `json:"override_address,omitempty"`
	// Deprecated: Use Route Action instead
	OverridePort  uint16 `json:"override_port,omitempty"`
	ProxyProtocol uint8  `json:"proxy_protocol,omitempty"`
}

type DirectOutboundOptions _DirectOutboundOptions

func (d *DirectOutboundOptions) UnmarshalJSONContext(ctx context.Context, content []byte) error {
	var options struct {
		_DirectOutboundOptions
		ProxyProtocolCompat *uint8 `json:"proxyProtocol,omitempty"`
	}
	err := json.UnmarshalDisallowUnknownFields(content, &options)
	if err != nil {
		return err
	}
	*d = DirectOutboundOptions(options._DirectOutboundOptions)
	if options.ProxyProtocolCompat != nil {
		if d.ProxyProtocol != 0 && d.ProxyProtocol != *options.ProxyProtocolCompat {
			return E.New("conflicting proxy protocol options")
		}
		d.ProxyProtocol = *options.ProxyProtocolCompat
	}
	//nolint:staticcheck
	if d.OverrideAddress != "" || d.OverridePort != 0 {
		return E.New("destination override fields in direct outbound are deprecated in sing-box 1.11.0 and removed in sing-box 1.13.0, use route options instead")
	}
	if d.ProxyProtocol > 2 {
		return E.New("invalid proxy protocol version: ", d.ProxyProtocol)
	}
	return nil
}
