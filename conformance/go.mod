module github.com/ashtonian/mqttv5/conformance

go 1.27.2

require (
	github.com/ashtonian/mqttv5 v0.15.0
	github.com/ashtonian/mqttv5/codec/json v0.15.0
	github.com/ashtonian/mqttv5/tlsrevocation v0.15.0
)

require golang.org/x/crypto v0.59.0 // indirect

replace (
	github.com/ashtonian/mqttv5 => ../
	github.com/ashtonian/mqttv5/codec/json => ../codec/json
	github.com/ashtonian/mqttv5/tlsrevocation => ../tlsrevocation
)
