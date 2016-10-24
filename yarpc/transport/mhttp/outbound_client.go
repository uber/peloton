package mhttp

import (
	"net"
	"net/http"
	"time"
)

func buildClient(cfg *outboundConfig) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			// options lifted from https://golang.org/src/net/http/transport.go
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: cfg.keepAlive,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}
