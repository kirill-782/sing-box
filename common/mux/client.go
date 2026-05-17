package mux

import (
	"context"
	"io"
	"net"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	sing_mux "github.com/sagernet/sing-mux"
	vmess "github.com/sagernet/sing-vmess"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const ProtocolMuxCool = "mux.cool"

type Client struct {
	sing *sing_mux.Client
	cool *muxCoolClient
}

var _ N.Dialer = (*Client)(nil)
var _ io.Closer = (*Client)(nil)

func NewClientWithOptions(dialer N.Dialer, logger logger.Logger, options option.OutboundMultiplexOptions) (*Client, error) {
	if !options.Enabled {
		return nil, nil
	}
	if options.Protocol == ProtocolMuxCool {
		if options.Padding {
			return nil, E.New("mux.cool: padding is not supported")
		}
		if options.Brutal != nil && options.Brutal.Enabled {
			return nil, E.New("mux.cool: brutal is not supported")
		}
		return &Client{
			cool: newMuxCoolClient(&clientDialer{dialer}, options.MaxConnections, options.MinStreams, options.MaxStreams),
		}, nil
	}
	var brutalOptions sing_mux.BrutalOptions
	if options.Brutal != nil && options.Brutal.Enabled {
		brutalOptions = sing_mux.BrutalOptions{
			Enabled:    true,
			SendBPS:    uint64(options.Brutal.UpMbps * C.MbpsToBps),
			ReceiveBPS: uint64(options.Brutal.DownMbps * C.MbpsToBps),
		}
		if brutalOptions.SendBPS < sing_mux.BrutalMinSpeedBPS {
			return nil, E.New("brutal: invalid upload speed")
		}
		if brutalOptions.ReceiveBPS < sing_mux.BrutalMinSpeedBPS {
			return nil, E.New("brutal: invalid download speed")
		}
	}
	client, err := sing_mux.NewClient(sing_mux.Options{
		Dialer:         &clientDialer{dialer},
		Logger:         logger,
		Protocol:       options.Protocol,
		MaxConnections: options.MaxConnections,
		MinStreams:     options.MinStreams,
		MaxStreams:     options.MaxStreams,
		Padding:        options.Padding,
		Brutal:         brutalOptions,
	})
	if err != nil {
		return nil, err
	}
	return &Client{sing: client}, nil
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if c.cool != nil {
		return c.cool.DialContext(ctx, network, destination)
	}
	return c.sing.DialContext(ctx, network, destination)
}

func (c *Client) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if c.cool != nil {
		return c.cool.ListenPacket(ctx, destination)
	}
	return c.sing.ListenPacket(ctx, destination)
}

func (c *Client) Reset() {
	if c.cool != nil {
		c.cool.Reset()
		return
	}
	c.sing.Reset()
}

func (c *Client) Close() error {
	if c.cool != nil {
		return c.cool.Close()
	}
	return c.sing.Close()
}

type clientDialer struct {
	N.Dialer
}

func (d *clientDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return d.Dialer.DialContext(adapter.OverrideContext(ctx), network, destination)
}

func (d *clientDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return d.Dialer.ListenPacket(adapter.OverrideContext(ctx), destination)
}

func (d *clientDialer) DialMuxContext(ctx context.Context) (net.Conn, error) {
	ctx = adapter.OverrideContext(ctx)
	if muxDialer, loaded := d.Dialer.(muxCoolDialer); loaded {
		return muxDialer.DialMuxContext(ctx)
	}
	return d.Dialer.DialContext(ctx, N.NetworkTCP, vmess.MuxDestination)
}
