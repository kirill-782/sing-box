package direct

import (
	"encoding/binary"
	"io"
	"net/netip"
	"strconv"

	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
)

var proxyProtocolV2Signature = []byte{'\r', '\n', '\r', '\n', 0, '\r', '\n', 'Q', 'U', 'I', 'T', '\n'}

func writeProxyProtocolHeaderTo(writer io.Writer, version uint8, source M.Socksaddr, destination M.Socksaddr) error {
	switch version {
	case 1, 2:
	default:
		return E.New("invalid proxy protocol version: ", version)
	}
	sourceAddress, destinationAddress, isIPv6, valid := proxyProtocolAddresses(source, destination)
	if !valid {
		return writeProxyProtocolUnknown(writer, version)
	}
	switch version {
	case 1:
		return writeProxyProtocolV1(writer, sourceAddress, destinationAddress, isIPv6, source.Port, destination.Port)
	default:
		return writeProxyProtocolV2(writer, sourceAddress, destinationAddress, isIPv6, source.Port, destination.Port)
	}
}

func proxyProtocolAddresses(source M.Socksaddr, destination M.Socksaddr) (netip.Addr, netip.Addr, bool, bool) {
	source = source.Unwrap()
	destination = destination.Unwrap()
	if !source.Addr.IsValid() || !destination.Addr.IsValid() {
		return netip.Addr{}, netip.Addr{}, false, false
	}
	if source.Addr.Is4() && destination.Addr.Is4() {
		return source.Addr, destination.Addr, false, true
	}
	sourceAddress := source.Addr
	if sourceAddress.Is4() {
		sourceAddress = netip.AddrFrom16(sourceAddress.As16())
	}
	destinationAddress := destination.Addr
	if destinationAddress.Is4() {
		destinationAddress = netip.AddrFrom16(destinationAddress.As16())
	}
	if !sourceAddress.Is6() || !destinationAddress.Is6() {
		return netip.Addr{}, netip.Addr{}, false, false
	}
	return sourceAddress, destinationAddress, true, true
}

func writeProxyProtocolUnknown(writer io.Writer, version uint8) error {
	switch version {
	case 1:
		_, err := io.WriteString(writer, "PROXY UNKNOWN\r\n")
		return err
	default:
		_, err := writer.Write([]byte{
			'\r', '\n', '\r', '\n', 0, '\r', '\n', 'Q', 'U', 'I', 'T', '\n',
			0x20, 0x00, 0x00, 0x00,
		})
		return err
	}
}

func writeProxyProtocolV1(writer io.Writer, sourceAddress netip.Addr, destinationAddress netip.Addr, isIPv6 bool, sourcePort uint16, destinationPort uint16) error {
	network := "TCP4"
	if isIPv6 {
		network = "TCP6"
	}
	_, err := io.WriteString(writer, "PROXY "+network+" "+sourceAddress.String()+" "+destinationAddress.String()+" "+strconv.Itoa(int(sourcePort))+" "+strconv.Itoa(int(destinationPort))+"\r\n")
	return err
}

func writeProxyProtocolV2(writer io.Writer, sourceAddress netip.Addr, destinationAddress netip.Addr, isIPv6 bool, sourcePort uint16, destinationPort uint16) error {
	addressLength := 12
	addressFamily := byte(0x11)
	if isIPv6 {
		addressLength = 36
		addressFamily = 0x21
	}
	header := make([]byte, 0, 16+addressLength)
	header = append(header, proxyProtocolV2Signature...)
	header = append(header, 0x21, addressFamily)
	header = binary.BigEndian.AppendUint16(header, uint16(addressLength))
	if isIPv6 {
		sourceBytes := sourceAddress.As16()
		destinationBytes := destinationAddress.As16()
		header = append(header, sourceBytes[:]...)
		header = append(header, destinationBytes[:]...)
	} else {
		sourceBytes := sourceAddress.As4()
		destinationBytes := destinationAddress.As4()
		header = append(header, sourceBytes[:]...)
		header = append(header, destinationBytes[:]...)
	}
	header = binary.BigEndian.AppendUint16(header, sourcePort)
	header = binary.BigEndian.AppendUint16(header, destinationPort)
	_, err := writer.Write(header)
	return err
}
