module github.com/ashtonian/mqttv5/benchmarks

go 1.26.4

require (
	github.com/ashtonian/mqttv5 v0.0.0
	github.com/eclipse/paho.golang v0.23.0
	github.com/eclipse/paho.mqtt.golang v1.5.1
	golang.org/x/perf v0.0.0-20260929162123-406019bb8b68
)

require (
	github.com/aclements/go-moremath v0.0.0-20210112150236-f10218a38794 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
)

replace github.com/ashtonian/mqttv5 => ..
