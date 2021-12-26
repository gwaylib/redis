package msq

import (
	"net"

	"github.com/gwaylib/errors"
)

func GetMAC() ([]string, error) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return nil, errors.As(err)
	}

	macAddrs := []string{}
	for _, netInterface := range netInterfaces {
		macAddr := netInterface.HardwareAddr.String()
		if len(macAddr) == 0 {
			continue
		}

		macAddrs = append(macAddrs, macAddr)
	}
	return macAddrs, nil
}
