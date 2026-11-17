module github.com/ashtonian/mqttv5/conformance

go 1.27.2

require (
	github.com/ashtonian/mqttv5 v0.16.0
	github.com/ashtonian/mqttv5/codec/json v0.16.0
	github.com/ashtonian/mqttv5/tlsrevocation v0.16.0
	github.com/ashtonian/mqttv5/transport/quic v0.16.0
	github.com/ashtonian/mqttv5/transport/ws v0.16.0
)

require (
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.4.0 // indirect
	github.com/quic-go/quic-go v0.65.0 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
)

replace (
	github.com/ashtonian/mqttv5 => ../
	github.com/ashtonian/mqttv5/codec/json => ../codec/json
	github.com/ashtonian/mqttv5/tlsrevocation => ../tlsrevocation
	github.com/ashtonian/mqttv5/transport/quic => ../transport/quic
	github.com/ashtonian/mqttv5/transport/ws => ../transport/ws
)
