module github.com/ashtonian/mqttv5/transport/quic

go 1.27.2

require (
	github.com/ashtonian/mqttv5 v0.16.1
	github.com/quic-go/quic-go v0.65.0
)

require (
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
)

replace github.com/ashtonian/mqttv5 => ../..
