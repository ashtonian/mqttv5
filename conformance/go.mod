module github.com/ashtonian/mqttv5/conformance

go 1.27.2

require (
	github.com/ashtonian/mqttv5 v0.13.0
	github.com/ashtonian/mqttv5/codec/json v0.13.0
)

replace (
	github.com/ashtonian/mqttv5 => ../
	github.com/ashtonian/mqttv5/codec/json => ../codec/json
)
