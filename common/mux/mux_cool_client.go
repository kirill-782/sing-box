package mux

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	vmess "github.com/sagernet/sing-vmess"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const muxCoolMaxPayload = 0xFFFF

type muxCoolClient struct {
	dialer         N.Dialer
	maxConnections int
	minStreams     int
	maxStreams     int

	access   sync.Mutex
	sessions []*muxCoolSession
	closed   bool
}

type muxCoolDialer interface {
	DialMuxContext(ctx context.Context) (net.Conn, error)
}

func newMuxCoolClient(dialer N.Dialer, maxConnections int, minStreams int, maxStreams int) *muxCoolClient {
	if maxStreams > 0 && minStreams > maxStreams {
		minStreams = maxStreams
	}
	if maxStreams == 0 && maxConnections == 0 {
		minStreams = 8
	}
	return &muxCoolClient{
		dialer:         dialer,
		maxConnections: maxConnections,
		minStreams:     minStreams,
		maxStreams:     maxStreams,
	}
}

func (c *muxCoolClient) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		session, err := c.pickSession(ctx)
		if err != nil {
			return nil, err
		}
		return session.openStream(destination)
	case N.NetworkUDP:
		session, err := c.pickSession(ctx)
		if err != nil {
			return nil, err
		}
		packetConn, err := session.openPacketConn(destination)
		if err != nil {
			return nil, err
		}
		return packetConn.(net.Conn), nil
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
}

func (c *muxCoolClient) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	session, err := c.pickSession(ctx)
	if err != nil {
		return nil, err
	}
	return session.openPacketConn(destination)
}

func (c *muxCoolClient) Reset() {
	c.access.Lock()
	sessions := c.sessions
	c.sessions = nil
	c.access.Unlock()
	for _, session := range sessions {
		session.Close()
	}
}

func (c *muxCoolClient) Close() error {
	c.access.Lock()
	if c.closed {
		c.access.Unlock()
		return nil
	}
	c.closed = true
	sessions := c.sessions
	c.sessions = nil
	c.access.Unlock()
	return common.Close(common.Map(sessions, func(it *muxCoolSession) any {
		return it
	})...)
}

func (c *muxCoolClient) pickSession(ctx context.Context) (*muxCoolSession, error) {
	c.access.Lock()
	if c.closed {
		c.access.Unlock()
		return nil, os.ErrClosed
	}

	sessions := make([]*muxCoolSession, 0, len(c.sessions))
	for _, session := range c.sessions {
		if !session.isClosed() {
			sessions = append(sessions, session)
		}
	}
	var selected *muxCoolSession
	selectedStreams := int32(-1)
	for _, session := range sessions {
		streams := session.active.Load()
		if selected == nil || streams < selectedStreams {
			selected = session
			selectedStreams = streams
		}
	}
	if selected != nil {
		numStreams := int(selected.active.Load())
		if numStreams == 0 {
			c.access.Unlock()
			return selected, nil
		}
		if c.maxConnections > 0 {
			if len(sessions) >= c.maxConnections || numStreams < c.minStreams {
				c.access.Unlock()
				return selected, nil
			}
		} else if c.maxStreams > 0 && numStreams < c.maxStreams {
			c.access.Unlock()
			return selected, nil
		}
	}
	c.access.Unlock()

	conn, err := c.dialMuxContext(ctx)
	if err != nil {
		return nil, err
	}
	session := newMuxCoolSession(conn, c.removeSession)
	c.access.Lock()
	if c.closed {
		c.access.Unlock()
		session.Close()
		return nil, os.ErrClosed
	}
	c.sessions = append(c.sessions, session)
	c.access.Unlock()
	go session.recvLoop()
	return session, nil
}

func (c *muxCoolClient) dialMuxContext(ctx context.Context) (net.Conn, error) {
	if dialer, loaded := c.dialer.(muxCoolDialer); loaded {
		return dialer.DialMuxContext(ctx)
	}
	return c.dialer.DialContext(ctx, N.NetworkTCP, vmess.MuxDestination)
}

func (c *muxCoolClient) removeSession(session *muxCoolSession) {
	c.access.Lock()
	defer c.access.Unlock()
	for index, it := range c.sessions {
		if it == session {
			c.sessions = append(c.sessions[:index], c.sessions[index+1:]...)
			return
		}
	}
}

type muxCoolSession struct {
	conn     net.Conn
	onClose  func(*muxCoolSession)
	streams  map[uint16]muxCoolStream
	nextID   uint32
	access   sync.Mutex
	write    sync.Mutex
	active   atomic.Int32
	closed   atomic.Bool
	closeErr error
}

type muxCoolStream interface {
	receive(payload []byte, destination M.Socksaddr) error
	closeRemote(err error)
}

func newMuxCoolSession(conn net.Conn, onClose func(*muxCoolSession)) *muxCoolSession {
	return &muxCoolSession{
		conn:    conn,
		onClose: onClose,
		streams: make(map[uint16]muxCoolStream),
	}
}

func (s *muxCoolSession) openStream(destination M.Socksaddr) (net.Conn, error) {
	stream := &muxCoolTCPStream{
		session:     s,
		destination: destination,
	}
	stream.reader, stream.writer = io.Pipe()
	id, err := s.register(stream)
	if err != nil {
		return nil, err
	}
	stream.id = id
	err = s.writeFrame(id, vmess.StatusNew, 0, vmess.NetworkTCP, destination, nil)
	if err != nil {
		stream.closeLocal(false, err)
		return nil, err
	}
	return stream, nil
}

func (s *muxCoolSession) openPacketConn(destination M.Socksaddr) (net.PacketConn, error) {
	conn := &muxCoolPacketConn{
		session:     s,
		destination: destination,
		inbound:     make(chan *N.PacketBuffer, 64),
		done:        make(chan struct{}),
	}
	id, err := s.register(conn)
	if err != nil {
		return nil, err
	}
	conn.id = id
	err = s.writeFrame(id, vmess.StatusNew, 0, vmess.NetworkUDP, destination, nil)
	if err != nil {
		conn.closeLocal(false, err)
		return nil, err
	}
	return conn, nil
}

func (s *muxCoolSession) register(stream muxCoolStream) (uint16, error) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed.Load() {
		if s.closeErr != nil {
			return 0, s.closeErr
		}
		return 0, os.ErrClosed
	}
	for {
		id := uint16(s.nextID + 1)
		s.nextID++
		if id == 0 {
			continue
		}
		if _, loaded := s.streams[id]; loaded {
			continue
		}
		s.streams[id] = stream
		s.active.Add(1)
		return id, nil
	}
}

func (s *muxCoolSession) unregister(id uint16) {
	s.access.Lock()
	if _, loaded := s.streams[id]; loaded {
		delete(s.streams, id)
		s.active.Add(-1)
	}
	s.access.Unlock()
}

func (s *muxCoolSession) getStream(id uint16) muxCoolStream {
	s.access.Lock()
	stream := s.streams[id]
	s.access.Unlock()
	return stream
}

func (s *muxCoolSession) isClosed() bool {
	return s.closed.Load()
}

func (s *muxCoolSession) Close() error {
	s.closeWithError(os.ErrClosed)
	return nil
}

func (s *muxCoolSession) recvLoop() {
	var err error
	for {
		err = s.recv()
		if err != nil {
			s.closeWithError(err)
			return
		}
	}
}

func (s *muxCoolSession) recv() error {
	var metadataLength uint16
	err := binary.Read(s.conn, binary.BigEndian, &metadataLength)
	if err != nil {
		return E.Cause(err, "read mux.cool frame metadata length")
	}
	if metadataLength < 4 {
		return E.New("mux.cool: bad frame metadata length: ", metadataLength)
	}
	metadata := make([]byte, metadataLength)
	_, err = io.ReadFull(s.conn, metadata)
	if err != nil {
		return E.Cause(err, "read mux.cool frame metadata")
	}
	id := binary.BigEndian.Uint16(metadata[:2])
	status := metadata[2]
	option := metadata[3]
	var destination M.Socksaddr
	if metadataLength > 4 {
		reader := bytes.NewReader(metadata[5:])
		destination, err = vmess.AddressSerializer.ReadAddrPort(reader)
		if err != nil {
			return E.Cause(err, "read mux.cool frame destination")
		}
		destination = destination.Unwrap()
	}
	var payload []byte
	if option&vmess.OptionData == vmess.OptionData {
		var payloadLength uint16
		err = binary.Read(s.conn, binary.BigEndian, &payloadLength)
		if err != nil {
			return E.Cause(err, "read mux.cool frame payload length")
		}
		if payloadLength > 0 {
			payload = make([]byte, payloadLength)
			_, err = io.ReadFull(s.conn, payload)
			if err != nil {
				return E.Cause(err, "read mux.cool frame payload")
			}
		}
	}
	stream := s.getStream(id)
	if stream == nil {
		return nil
	}
	if option&vmess.OptionError == vmess.OptionError {
		err = E.Cause(net.ErrClosed, "remote closed")
	}
	switch status {
	case vmess.StatusKeep:
		if len(payload) > 0 {
			err = stream.receive(payload, destination)
			if err != nil {
				stream.closeRemote(err)
			}
		}
	case vmess.StatusEnd:
		if len(payload) > 0 {
			_ = stream.receive(payload, destination)
		}
		stream.closeRemote(err)
	case vmess.StatusKeepAlive:
	case vmess.StatusNew:
		return E.New("mux.cool: unexpected new frame from server")
	default:
		return E.New("mux.cool: bad frame status: ", status)
	}
	return nil
}

func (s *muxCoolSession) writeTCPData(id uint16, payload []byte) error {
	for len(payload) > 0 {
		chunk := payload
		if len(chunk) > muxCoolMaxPayload {
			chunk = chunk[:muxCoolMaxPayload]
		}
		err := s.writeFrame(id, vmess.StatusKeep, vmess.OptionData, 0, M.Socksaddr{}, chunk)
		if err != nil {
			return err
		}
		payload = payload[len(chunk):]
	}
	return nil
}

func (s *muxCoolSession) writePacketData(id uint16, payload []byte, destination M.Socksaddr) error {
	if len(payload) > muxCoolMaxPayload {
		return E.New("mux.cool: packet too large: ", len(payload))
	}
	return s.writeFrame(id, vmess.StatusKeep, vmess.OptionData, vmess.NetworkUDP, destination, payload)
}

func (s *muxCoolSession) writeClose(id uint16, hasError bool) error {
	var option byte
	if hasError {
		option = vmess.OptionError
	}
	return s.writeFrame(id, vmess.StatusEnd, option, 0, M.Socksaddr{}, nil)
}

func (s *muxCoolSession) writeFrame(id uint16, status byte, option byte, network byte, destination M.Socksaddr, payload []byte) error {
	if s.closed.Load() {
		return os.ErrClosed
	}
	hasPayload := option&vmess.OptionData == vmess.OptionData
	if len(payload) > 0 {
		option |= vmess.OptionData
		hasPayload = true
	}
	var frame bytes.Buffer
	metadataLength := 4
	if network != 0 {
		metadataLength += 1 + vmess.AddressSerializer.AddrPortLen(destination)
	}
	common.Must(
		binary.Write(&frame, binary.BigEndian, uint16(metadataLength)),
		binary.Write(&frame, binary.BigEndian, id),
		frame.WriteByte(status),
		frame.WriteByte(option),
	)
	if network != 0 {
		common.Must(frame.WriteByte(network))
		err := vmess.AddressSerializer.WriteAddrPort(&frame, destination)
		if err != nil {
			return err
		}
	}
	if hasPayload {
		common.Must(binary.Write(&frame, binary.BigEndian, uint16(len(payload))))
		if len(payload) > 0 {
			_, err := frame.Write(payload)
			if err != nil {
				return err
			}
		}
	}
	s.write.Lock()
	defer s.write.Unlock()
	_, err := s.conn.Write(frame.Bytes())
	return err
}

func (s *muxCoolSession) closeWithError(err error) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.closeErr = err
	_ = s.conn.Close()
	s.access.Lock()
	streams := s.streams
	s.streams = make(map[uint16]muxCoolStream)
	s.active.Store(0)
	s.access.Unlock()
	for _, stream := range streams {
		stream.closeRemote(err)
	}
	if s.onClose != nil {
		s.onClose(s)
	}
}

type muxCoolTCPStream struct {
	session     *muxCoolSession
	id          uint16
	destination M.Socksaddr
	reader      *io.PipeReader
	writer      *io.PipeWriter
	closeOnce   sync.Once
}

var _ net.Conn = (*muxCoolTCPStream)(nil)

func (s *muxCoolTCPStream) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

func (s *muxCoolTCPStream) Write(p []byte) (int, error) {
	err := s.session.writeTCPData(s.id, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *muxCoolTCPStream) WriteBuffer(buffer *buf.Buffer) error {
	defer buffer.Release()
	return common.Error(s.Write(buffer.Bytes()))
}

func (s *muxCoolTCPStream) FrontHeadroom() int {
	return 8
}

func (s *muxCoolTCPStream) Upstream() any {
	return s.session.conn
}

func (s *muxCoolTCPStream) Close() error {
	return s.closeLocal(true, nil)
}

func (s *muxCoolTCPStream) LocalAddr() net.Addr {
	return s.session.conn.LocalAddr()
}

func (s *muxCoolTCPStream) RemoteAddr() net.Addr {
	return s.destination
}

func (s *muxCoolTCPStream) SetDeadline(time.Time) error {
	return os.ErrInvalid
}

func (s *muxCoolTCPStream) SetReadDeadline(time.Time) error {
	return os.ErrInvalid
}

func (s *muxCoolTCPStream) SetWriteDeadline(time.Time) error {
	return os.ErrInvalid
}

func (s *muxCoolTCPStream) receive(payload []byte, _ M.Socksaddr) error {
	_, err := s.writer.Write(payload)
	return err
}

func (s *muxCoolTCPStream) closeRemote(err error) {
	s.closeOnce.Do(func() {
		s.session.unregister(s.id)
		_ = s.writer.CloseWithError(err)
		_ = s.reader.CloseWithError(err)
	})
}

func (s *muxCoolTCPStream) closeLocal(sendEnd bool, closeErr error) error {
	var err error
	s.closeOnce.Do(func() {
		s.session.unregister(s.id)
		if sendEnd {
			err = s.session.writeClose(s.id, closeErr != nil)
		}
		_ = s.writer.CloseWithError(closeErr)
		_ = s.reader.CloseWithError(closeErr)
	})
	return err
}

type muxCoolPacketConn struct {
	session     *muxCoolSession
	id          uint16
	destination M.Socksaddr
	inbound     chan *N.PacketBuffer
	done        chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

var (
	_ net.PacketConn  = (*muxCoolPacketConn)(nil)
	_ net.Conn        = (*muxCoolPacketConn)(nil)
	_ N.NetPacketConn = (*muxCoolPacketConn)(nil)
)

func (c *muxCoolPacketConn) Read(p []byte) (int, error) {
	n, _, err := c.ReadFrom(p)
	return n, err
}

func (c *muxCoolPacketConn) Write(p []byte) (int, error) {
	return c.WriteTo(p, c.destination)
}

func (c *muxCoolPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	buffer := buf.With(p)
	var destination M.Socksaddr
	destination, err = c.ReadPacket(buffer)
	if err != nil {
		return
	}
	n = buffer.Len()
	if destination.IsDomain() {
		addr = destination
	} else {
		addr = destination.UDPAddr()
	}
	return
}

func (c *muxCoolPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	err = c.WritePacket(buf.As(p), M.SocksaddrFromNet(addr))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *muxCoolPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	select {
	case packet := <-c.inbound:
		defer N.PutPacketBuffer(packet)
		defer packet.Buffer.Release()
		if packet.Buffer.Len() > buffer.FreeLen() {
			return M.Socksaddr{}, E.Extend(io.ErrShortBuffer, "mux.cool need ", packet.Buffer.Len())
		}
		_, err := buffer.Write(packet.Buffer.Bytes())
		if err != nil {
			return M.Socksaddr{}, err
		}
		return packet.Destination, nil
	case <-c.done:
		if c.closeErr != nil {
			return M.Socksaddr{}, c.closeErr
		}
		return M.Socksaddr{}, net.ErrClosed
	}
}

func (c *muxCoolPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	defer buffer.Release()
	if !destination.IsValid() {
		destination = c.destination
	}
	return c.session.writePacketData(c.id, buffer.Bytes(), destination)
}

func (c *muxCoolPacketConn) Close() error {
	return c.closeLocal(true, nil)
}

func (c *muxCoolPacketConn) LocalAddr() net.Addr {
	return c.session.conn.LocalAddr()
}

func (c *muxCoolPacketConn) RemoteAddr() net.Addr {
	return c.destination
}

func (c *muxCoolPacketConn) SetDeadline(time.Time) error {
	return os.ErrInvalid
}

func (c *muxCoolPacketConn) SetReadDeadline(time.Time) error {
	return os.ErrInvalid
}

func (c *muxCoolPacketConn) SetWriteDeadline(time.Time) error {
	return os.ErrInvalid
}

func (c *muxCoolPacketConn) receive(payload []byte, destination M.Socksaddr) error {
	if !destination.IsValid() {
		destination = c.destination
	}
	packet := N.NewPacketBuffer()
	packet.Buffer = buf.As(payload).ToOwned()
	packet.Destination = destination
	select {
	case c.inbound <- packet:
		return nil
	case <-c.done:
		packet.Buffer.Release()
		N.PutPacketBuffer(packet)
		if c.closeErr != nil {
			return c.closeErr
		}
		return net.ErrClosed
	}
}

func (c *muxCoolPacketConn) closeRemote(err error) {
	c.closeLocal(false, err)
}

func (c *muxCoolPacketConn) closeLocal(sendEnd bool, closeErr error) error {
	var err error
	c.closeOnce.Do(func() {
		c.closeErr = closeErr
		c.session.unregister(c.id)
		if sendEnd {
			err = c.session.writeClose(c.id, closeErr != nil)
		}
		close(c.done)
	})
	return err
}

var _ N.ExtendedWriter = (*muxCoolTCPStream)(nil)
