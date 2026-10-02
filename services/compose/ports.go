package compose

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/compose-spec/compose-go/v2/types"
)

type publishedPort struct {
	service string
	host    netip.Addr // Invalid means Docker's default binding on all addresses.
	target  uint32
}

type portBindings map[uint16][]publishedPort

func (bindings portBindings) add(service string, port types.ServicePortConfig) error {
	published, err := strconv.ParseUint(port.Published, 10, 16)
	if err != nil || published == 0 {
		return fmt.Errorf("services.%s.ports requires fixed published ports", service)
	}
	if published == 49983 {
		return fmt.Errorf("services.%s.ports: port 49983 is reserved for envd", service)
	}
	if port.Protocol != "tcp" && port.Protocol != "" {
		return fmt.Errorf("services.%s.ports: only TCP publishing is supported", service)
	}
	var host netip.Addr
	if port.HostIP != "" {
		host, err = netip.ParseAddr(strings.Trim(port.HostIP, "[]"))
		if err != nil {
			return fmt.Errorf("services.%s.ports: invalid host IP %q", service, port.HostIP)
		}
		host = host.Unmap()
	}
	for _, previous := range bindings[uint16(published)] {
		if previous.service == service && previous.host == host && previous.target == port.Target {
			return nil // Identical mappings within one service are redundant.
		}
		overlaps := !host.IsValid() || !previous.host.IsValid() || host == previous.host ||
			(host.Is4() == previous.host.Is4() && (host.IsUnspecified() || previous.host.IsUnspecified()))
		if overlaps {
			return fmt.Errorf("services.%s.ports: published TCP port %d conflicts with services.%s", service, published, previous.service)
		}
	}
	bindings[uint16(published)] = append(bindings[uint16(published)], publishedPort{service, host, port.Target})
	return nil
}
