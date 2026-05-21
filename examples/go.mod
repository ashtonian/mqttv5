module github.com/ashtonian/mqttv5/examples

go 1.26.3

require (
	github.com/ashtonian/mqttv5 v0.0.0
	github.com/ashtonian/mqttv5/codec/json v0.0.0
	github.com/ashtonian/mqttv5/transport/ws v0.0.0
)

replace (
	github.com/ashtonian/mqttv5 => ../
	github.com/ashtonian/mqttv5/codec/json => ../codec/json
	github.com/ashtonian/mqttv5/transport/ws => ../transport/ws
)
