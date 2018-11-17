package apibus

import (
	"net"
	"strings"
)

// 获得出口网卡的ip地址
func getOutgoingIp() (string, error) {
	conn, err := net.Dial("udp", "114.114.114.114:53")
	if err != nil {
		return "", err
	}

	localAddr := conn.LocalAddr().String()
	return localAddr[:strings.IndexByte(localAddr, ':')], nil
}
