module github.com/ashtonian/mqttv5/conformance

go 1.27.2

require (
	github.com/ashtonian/mqttv5 v0.15.0
	github.com/ashtonian/mqttv5/codec/json v0.15.0
	github.com/ashtonian/mqttv5/tlsrevocation v0.15.0
	github.com/ashtonian/mqttv5/transport/quic v0.15.0
)

require (
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
)
