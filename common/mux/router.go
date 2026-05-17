package mux

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	sing_mux "github.com/sagernet/sing-mux"
	vmess "github.com/sagernet/sing-vmess"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
)

type Router struct {
	router  adapter.ConnectionRouterEx
	service *sing_mux.Service
	logger  logger.ContextLogger
}

func NewRouterWithOptions(router adapter.ConnectionRouterEx, logger logger.ContextLogger, options option.InboundMultiplexOptions) (adapter.ConnectionRouterEx, error) {
	if !options.Enabled {
		return router, nil
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
	service, err := sing_mux.NewService(sing_mux.ServiceOptions{
		NewStreamContext: func(ctx context.Context, conn net.Conn) context.Context {
			return log.ContextWithNewID(ctx)
		},
		Logger:    logger,
		HandlerEx: adapter.NewRouteContextHandler(router),
		Padding:   options.Padding,
		Brutal:    brutalOptions,
	})
	if err != nil {
		return nil, err
	}
	return &Router{router, service, logger}, nil
}

// Deprecated: Use RouteConnectionEx instead.
func (r *Router) RouteConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext) error {
	if metadata.Destination == sing_mux.Destination {
		// TODO: check if WithContext is necessary
		return r.service.NewConnection(adapter.WithContext(ctx, &metadata), conn, adapter.UpstreamMetadata(metadata))
	} else if metadata.Destination == vmess.MuxDestination {
		r.logger.InfoContext(ctx, "inbound Mux.Cool connection")
		metadata.Domain = metadata.Destination.Fqdn
		return vmess.HandleMuxConnection(adapter.WithContext(ctx, &metadata), conn, metadata.Source, adapter.NewRouteContextHandler(r.router))
	} else {
		return r.router.RouteConnection(ctx, conn, metadata)
	}
}

// Deprecated: Use RoutePacketConnectionEx instead.
func (r *Router) RoutePacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext) error {
	return r.router.RoutePacketConnection(ctx, conn, metadata)
}

func (r *Router) RouteConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	if metadata.Destination == sing_mux.Destination {
		r.service.NewConnectionEx(adapter.WithContext(ctx, &metadata), conn, metadata.Source, metadata.Destination, onClose)
		return
	}
	if metadata.Destination == vmess.MuxDestination {
		r.logger.InfoContext(ctx, "inbound Mux.Cool connection")
		metadata.Domain = metadata.Destination.Fqdn
		err := vmess.HandleMuxConnection(adapter.WithContext(ctx, &metadata), conn, metadata.Source, adapter.NewRouteContextHandler(r.router))
		if err != nil && !E.IsClosedOrCanceled(err) {
			r.logger.ErrorContext(ctx, E.Cause(err, "process Mux.Cool connection"))
		}
		common.Close(conn)
		if onClose != nil {
			onClose(err)
		}
		return
	}
	r.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}

func (r *Router) RoutePacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.router.RoutePacketConnectionEx(ctx, conn, metadata, onClose)
}
