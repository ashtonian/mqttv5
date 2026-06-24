module github.com/ashtonian/mqttv5/conformance

go 1.26.4

require (
	github.com/ashtonian/mqttv5 v0.0.0
	github.com/ashtonian/mqttv5/codec/json v0.0.0
)

replace (
	github.com/ashtonian/mqttv5 => ../
	github.com/ashtonian/mqttv5/codec/json => ../codec/json
)
